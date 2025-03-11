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

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{
    block::BlockIterator,
    iterators::StorageIterator,
    key::{Key, KeySlice},
    lsm_storage::BlockCache,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let first_block = table.read_block_cached(0)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(first_block);
        let table_iter = Self {
            table,
            blk_iter,
            blk_idx: 0,
        };
        Ok(table_iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let first_block = self.table.read_block_cached(0)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(first_block);
        self.blk_idx = 0;
        self.blk_iter = blk_iter;
        Ok(())
    }

    fn seek_key(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut idx = table.find_block_idx(key);
        let block = table.read_block_cached(idx)?;
        let mut blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        if !blk_iter.is_valid() {
            idx += 1;
            if idx < table.num_of_blocks() {
                let block = table.read_block_cached(idx)?;
                blk_iter = BlockIterator::create_and_seek_to_first(block);
            }
        }
        Ok((idx, blk_iter))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_key(&table, key)?;
        Ok(Self {
            table,
            blk_idx,
            blk_iter,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_key(&self.table, key)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx)?,
                );
            }
        }
        Ok(())
    }
}
