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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, U16_SIZE};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn compute_overlap(s1: &KeySlice, s2: &KeySlice) -> usize {
    let mut ret = 0;
    loop {
        if ret >= s1.len() || ret >= s2.len() {
            break;
        }
        if s1.raw_ref()[ret] != s2.raw_ref()[ret] {
            break;
        }
        ret += 1;
    }
    ret
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        U16_SIZE + self.offsets.len() * U16_SIZE + self.data.len()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if !self.is_empty()
            && self.estimated_size() + key.len() + value.len() + 3 * U16_SIZE > self.block_size
        {
            return false;
        }

        // | overlap | stored key len | key data | value len | value data |
        self.offsets.push(self.data.len() as u16);
        let overlap = compute_overlap(&self.first_key.as_key_slice(), &key);
        self.data.put_u16(overlap as u16);
        self.data.put_u16((key.len() - overlap) as u16);
        let pre_trim_key = &key.raw_ref()[overlap..];
        self.data.put(pre_trim_key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
