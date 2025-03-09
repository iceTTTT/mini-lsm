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

use bytes::Buf;

use crate::{
    block::U16_SIZE,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        let mut entry = self.data.as_slice();
        entry.get_u16();
        let key_size = entry.get_u16() as usize;
        KeyVec::from_vec(entry[..key_size].to_vec())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_first_key(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(self.is_valid(), "invalid block iterator");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid(), "invalid block iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    pub fn seek_to_idx(&mut self, idx: usize) {
        if self.block.offsets.len() <= idx {
            self.key.clear();
            self.value_range = (0, 0);
            self.idx = idx;
            return;
        }
        let offset_set = self.block.offsets[idx] as usize;
        let mut entry = &self.block.data[offset_set..];
        // key
        self.key.clear();
        let overlap_len = entry.get_u16() as usize;
        let overlap_part = &self.first_key.raw_ref()[..overlap_len];
        self.key.append(overlap_part);
        let key_len = entry.get_u16() as usize;
        let key_part = &entry[..key_len];
        self.key.append(key_part);
        entry.advance(key_len);
        // value_range
        let value_len = entry.get_u16() as usize;
        let value_start = offset_set + U16_SIZE * 3 + key_len;
        let value_end = value_start + value_len;
        self.value_range = (value_start, value_end);
        // idx
        self.idx = idx;
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left != right {
            let mid = left + (right - left) / 2;
            self.seek_to_idx(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Equal => return,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Less => left = mid + 1,
            }
        }
        self.seek_to_idx(right);
    }
}
