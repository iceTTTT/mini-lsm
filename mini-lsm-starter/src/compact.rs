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

mod leveled;
mod simple_leveled;
mod tiered;
use std::collections::HashSet;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn generate_new_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        is_compact_bottom: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_ssts = Vec::new();
        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            if is_compact_bottom {
                if !iter.value().is_empty() {
                    builder.as_mut().unwrap().add(iter.key(), iter.value());
                }
            } else {
                builder.as_mut().unwrap().add(iter.key(), iter.value());
            }
            iter.next()?;

            if builder.as_ref().unwrap().estimated_size() >= self.options.target_sst_size {
                let builder = builder.take().unwrap();
                let sst_id = self.next_sst_id();
                let sst = builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?;
                new_ssts.push(Arc::new(sst));
            }
        }
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id();
            let sst = builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            new_ssts.push(Arc::new(sst));
        }
        Ok(new_ssts)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // merge iter for l0.
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables.iter() {
                    let table = state.sstables.get(id).unwrap();
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        table.clone(),
                    )?));
                }
                let mut l1_tables = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    let table = state.sstables.get(id).unwrap();
                    l1_tables.push(table.clone());
                }
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_tables)?,
                )?;
                // concat iter for l1.
                self.generate_new_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                None => {
                    let mut l0_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids {
                        let table = state.sstables.get(id).unwrap();
                        l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            table.clone(),
                        )?));
                    }
                    let mut l1_tables = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids {
                        l1_tables.push(state.sstables.get(id).unwrap().clone());
                    }
                    let iter = TwoMergeIterator::create(
                        MergeIterator::create(l0_iters),
                        SstConcatIterator::create_and_seek_to_first(l1_tables)?,
                    )?;
                    self.generate_new_sst_from_iter(iter, task.compact_to_bottom_level())
                }
                Some(_) => {
                    let mut upper_tables = Vec::with_capacity(upper_level_sst_ids.len());
                    let mut lower_tables = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in upper_level_sst_ids {
                        upper_tables.push(state.sstables.get(id).unwrap().clone());
                    }
                    for id in lower_level_sst_ids {
                        lower_tables.push(state.sstables.get(id).unwrap().clone());
                    }
                    let iter = TwoMergeIterator::create(
                        SstConcatIterator::create_and_seek_to_first(upper_tables)?,
                        SstConcatIterator::create_and_seek_to_first(lower_tables)?,
                    )?;
                    self.generate_new_sst_from_iter(iter, task.compact_to_bottom_level())
                }
            },
            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                let mut con_iters = Vec::new();
                for (_, tier) in tiers {
                    let mut tables = Vec::new();
                    for tid in tier {
                        let table = state.sstables.get(tid).unwrap().clone();
                        tables.push(table);
                    }
                    con_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        tables,
                    )?));
                }
                self.generate_new_sst_from_iter(
                    MergeIterator::create(con_iters),
                    task.compact_to_bottom_level(),
                )
            }
            _ => {
                panic!("not impl")
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("only no compaction will forc full compaction")
        };
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let l0_tables = state.l0_sstables.clone();
        let l1_tables = state.levels[0].1.clone();
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_tables.clone(),
            l1_sstables: l1_tables.clone(),
        };
        let new_ssts = self.compact(&task)?;
        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            // remove sstables.
            for id in l0_tables.iter().chain(l1_tables.iter()) {
                state.sstables.remove(id).unwrap();
            }
            let mut new_l1 = Vec::with_capacity(new_ssts.len());
            // insert sstables.
            for new_sst in new_ssts {
                new_l1.push(new_sst.sst_id());
                let res = state.sstables.insert(new_sst.sst_id(), new_sst);
                assert!(res.is_none());
            }
            // l1_vector
            state.levels[0].1.clone_from(&new_l1);
            // l0_vector
            let mut l0_map = l0_tables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(task, new_l1.clone()),
            )?;
        }
        // remove files. because no belongs to state.
        for id in l0_tables.iter().chain(l1_tables.iter()) {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(state.as_ref());
        let Some(task) = task else {
            return Ok(());
        };
        let new_tables = self.compact(&task)?;
        let remove_table_ids = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            let mut new_table_ids = Vec::with_capacity(new_tables.len());
            // insert to hash map
            for table in new_tables {
                new_table_ids.push(table.sst_id());
                snapshot.sstables.insert(table.sst_id(), table);
            }
            let (mut snapshot, remove_table_ids) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &new_table_ids, false);
            // remove from hash map
            for id in &remove_table_ids {
                snapshot.sstables.remove(id);
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            drop(state);
            self.sync_dir()?;
            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(task, new_table_ids.clone()),
            )?;
            remove_table_ids
        };
        // remove files
        for id in &remove_table_ids {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let flush = {
            let guard = self.state.read();
            guard.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if flush {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
