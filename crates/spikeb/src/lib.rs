//! Spike B: bytes-per-key measurement of two hash-table layouts.
//!
//! `pointer` mimics the classic Redis dict (bucket array of pointers, one heap
//! allocation per entry, key and value). `compact` is the layout under
//! evaluation (8-byte index slots, size-class slabs, one record per key).

use std::fmt;

pub mod compact;
pub mod mem;
pub mod pointer;
pub mod workload;

/// Longest key either layout accepts (the compact record stores it in one byte).
pub const MAX_KEY_LEN: usize = 255;
/// Longest value either layout accepts (the compact record stores it in a u16).
pub const MAX_VALUE_LEN: usize = 65535;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableError {
    KeyTooLong {
        len: usize,
    },
    ValueTooLong {
        len: usize,
    },
    /// The compact slab ran out of addressable chunks.
    Full,
}

impl fmt::Display for TableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyTooLong { len } => write!(f, "key of {len} bytes exceeds {MAX_KEY_LEN}"),
            Self::ValueTooLong { len } => {
                write!(f, "value of {len} bytes exceeds {MAX_VALUE_LEN}")
            }
            Self::Full => write!(f, "slab is full"),
        }
    }
}

impl std::error::Error for TableError {}

pub trait Table {
    /// Insert or overwrite. The expiry is replaced too (`None` clears it).
    fn insert(&mut self, key: &[u8], value: &[u8], expiry: Option<u64>) -> Result<(), TableError>;
    fn get(&self, key: &[u8]) -> Option<&[u8]>;
    /// Stored expiry; `None` when the key is absent or has no expiry.
    fn expiry(&self, key: &[u8]) -> Option<u64>;
    fn remove(&mut self, key: &[u8]) -> bool;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Layout-specific `key=value` fields appended to the result line.
    fn stats(&self) -> String {
        String::new()
    }
}

pub(crate) fn validate(key: &[u8], value: &[u8]) -> Result<(), TableError> {
    if key.len() > MAX_KEY_LEN {
        return Err(TableError::KeyTooLong { len: key.len() });
    }
    if value.len() > MAX_VALUE_LEN {
        return Err(TableError::ValueTooLong { len: value.len() });
    }
    Ok(())
}

pub fn default_hash(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}
