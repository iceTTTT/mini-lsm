#![allow(dead_code)]
// REMOVE THIS LINE after fully implementing this functionality
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

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(path)
                    .context("failed to open wal file")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path.as_ref())
            .context("fail open")?;
        let mut vec = Vec::new();
        file.read_to_end(&mut vec)?;
        let mut buf = vec.as_slice();
        while buf.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            let key_len = buf.get_u16() as usize;
            hasher.write_u16(key_len as u16);
            let key = Bytes::copy_from_slice(&buf[..key_len]);
            hasher.write(&key);
            buf.advance(key_len);
            let value_len = buf.get_u16() as usize;
            hasher.write_u16(value_len as u16);
            let value = Bytes::copy_from_slice(&buf[..value_len]);
            hasher.write(&value);
            buf.advance(value_len);
            let checksum = buf.get_u32();
            if checksum != hasher.finalize() {
                bail!("checksum mismatch");
            }
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut vec = Vec::with_capacity(key.len() + value.len() + 4 * size_of::<u16>());
        let mut hasher = crc32fast::Hasher::new();
        vec.put_u16(key.len() as u16);
        vec.put_slice(key);
        vec.put_u16(value.len() as u16);
        vec.put_slice(value);
        hasher.write_u16(key.len() as u16);
        hasher.write(key);
        hasher.write_u16(value.len() as u16);
        hasher.write(value);
        vec.put_u32(hasher.finalize());
        file.write_all(&vec)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
