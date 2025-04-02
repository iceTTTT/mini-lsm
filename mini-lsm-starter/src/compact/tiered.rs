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

use std::collections::HashMap;
use std::usize;

use serde::{Deserialize, Serialize};

use crate::{iterators, lsm_storage::LsmStorageState};

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(snapshot.l0_sstables.is_empty(), "tired compaction has no ");
        if self.options.num_tiers > snapshot.levels.len() {
            return None;
        }
        // space amplification
        let mut upper_size = 0;
        for tier_id in 0..snapshot.levels.len() - 1 {
            let (_, tier) = snapshot.levels.get(tier_id).unwrap();
            upper_size += tier.len();
        }
        if (upper_size as f64 / snapshot.levels.last().unwrap().1.len() as f64)
            > (self.options.max_size_amplification_percent as f64 / 100.0)
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        // size ratio
        let mut pre_size = 0;
        let size_trigger = (self.options.size_ratio as f64 + 100.0) / 100.0;
        for tier_id in 0..snapshot.levels.len() - 1 {
            let (_, upper) = snapshot.levels.get(tier_id).unwrap();
            pre_size += upper.len();
            let (_, lower) = snapshot.levels.get(tier_id + 1).unwrap();
            let lower_size = lower.len();
            if (lower_size as f64 / pre_size as f64) > size_trigger
                && tier_id + 1 > self.options.min_merge_width
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(tier_id + 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: false,
                });
            }
        }
        // reduce runs
        let compact_num = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(compact_num)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: compact_num >= snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut sst_to_remove = Vec::new();
        let mut remove_map = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut new_levels = Vec::new();
        let mut added_new = false;
        for (id, iter) in &snapshot.levels {
            if let Some(pend_delete_iter) = remove_map.remove(id) {
                sst_to_remove.extend(pend_delete_iter);
            } else {
                new_levels.push((*id, iter.clone()));
            }

            if remove_map.is_empty() && !added_new {
                added_new = true;
                new_levels.push((output[0], output.to_vec()));
            }
        }
        if !remove_map.is_empty() {
            unreachable!("some tiers not found??");
        }
        snapshot.levels = new_levels;
        (snapshot, sst_to_remove)
    }
}
