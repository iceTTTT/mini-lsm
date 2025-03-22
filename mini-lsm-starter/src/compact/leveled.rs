// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{self, HashSet};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
        .iter()
        .map(|x| snapshot.sstables[x].first_key())
        .min()
        .cloned()
        .unwrap();
        let end_key = sst_ids
        .iter()
        .map(|x| snapshot.sstables[x].last_key())
        .max()
        .cloned()
        .unwrap();
        let mut matched_sst = Vec::new();
        for id in &snapshot.levels[in_level - 1].1 {
            let table = &snapshot.sstables[id];
            if !(table.first_key() > &end_key || table.last_key() < &begin_key) {
                matched_sst.push(*id);
            }
        }
        matched_sst
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut real_size = Vec::with_capacity(self.options.max_levels);
        let mut target_size = (0..self.options.max_levels).map(|_|0 as usize).collect::<Vec<_>>();
        let mut base_level = self.options.max_levels;
        // calc real size
        for id in 0..self.options.max_levels {
            real_size.push(snapshot.levels[id]
            .1
            .iter()
            .map(|x| snapshot.sstables[x].table_size())
            .sum::<u64>() as usize
            );
        }
        // calc target size
        let base_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        target_size[real_size.len() - 1] = base_size_bytes.max(real_size[real_size.len() - 1]);
        for id in 0..self.options.max_levels - 1 {
            if target_size[id + 1] > base_size_bytes {
                target_size[id] = target_size[id + 1] / self.options.level_size_multiplier;
            }
            if target_size[id] > 0 {
                base_level = id + 1;
            }
        }

        // check l0.
        if snapshot.l0_sstables.len() > self.options.level0_file_num_compaction_trigger {
            return Some(
                LeveledCompactionTask {
                    upper_level: None, 
                    upper_level_sst_ids: snapshot.l0_sstables.clone(),
                    lower_level: base_level,
                    lower_level_sst_ids: self.find_overlapping_ssts(snapshot, &snapshot.l0_sstables, base_level),
                    is_lower_level_bottom_level: base_level == self.options.max_levels,
                }
            );
        }
        
        // cal priority and compact max prior level.
        let mut prior = Vec::new();
        for id in base_level - 1..self.options.max_levels {
            let priority = real_size[id] as f64 / target_size[id] as f64;
            if priority > 1.0 {
                prior.push((priority, id + 1));
            }
        }
        prior.sort_by(|x,y| x.partial_cmp(y).unwrap().reverse());
        let prior = prior.first();
        if let Some((_, level)) = prior {
            let level = *level;
            let sst_id = snapshot.levels[level - 1].1.iter().min().copied().unwrap();
            return Some(
                LeveledCompactionTask {
                    upper_level: Some(level),
                    upper_level_sst_ids: vec![sst_id],
                    lower_level: level + 1,
                    lower_level_sst_ids: self.find_overlapping_ssts(snapshot, &[sst_id], level + 1),
                    is_lower_level_bottom_level: level + 1 == self.options.max_levels,
                }
            );
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut ssts_to_remove = Vec::new();
        ssts_to_remove.extend(&task.upper_level_sst_ids);
        ssts_to_remove.extend(&task.lower_level_sst_ids);
        let mut upper_to_remove = task.upper_level_sst_ids.iter().copied().collect::<HashSet<_>>();
        let mut lower_to_remove = task.lower_level_sst_ids.iter().copied().collect::<HashSet<_>>();
        if let Some(upper) = task.upper_level {
            let new_upper_ssts = snapshot.levels[upper - 1].1
                .iter()
                .filter_map(|x| 
                {  
                    if upper_to_remove.remove(x) {
                        return None;
                    }
                    Some(*x)
                }
                ).collect::<Vec<_>>();
            assert!(upper_to_remove.is_empty());
            snapshot.levels[upper - 1].1 = new_upper_ssts;
        } else {
            let new_l0_ssts = snapshot.l0_sstables
            .iter()
            .filter_map(|x|
            {
                if upper_to_remove.remove(x) {
                    return None;
                }
                Some(*x)
            }
            ).collect::<Vec<_>>();
            assert!(upper_to_remove.is_empty());
            snapshot.l0_sstables = new_l0_ssts;
        }
        let mut new_lower_ssts = snapshot.levels[task.lower_level - 1].1
            .iter()
            .filter_map(|x|
            {
                if lower_to_remove.remove(x) {
                    return None;
                }
                Some(*x)
            }
            ).collect::<Vec<_>>();
        assert!(lower_to_remove.is_empty());
        new_lower_ssts.extend(output);
        if !in_recovery {
            new_lower_ssts.sort_by(|x, y| 
                snapshot.sstables[x].first_key()
                .cmp(snapshot.sstables[y].first_key()));
        }
        snapshot.levels[task.lower_level - 1].1 = new_lower_ssts;
        (snapshot, ssts_to_remove)
    }
}
