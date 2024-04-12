pub mod btree;

use crate::bitcask::data::log_record::LogRecordPos;

/// A trait for data indexing abstraction.
pub trait Indexer: Sync + Send {
    /// Write KEY to INDEXER at position POS.
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;

    /// Read KEY from INDEXER.
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// Delete the index associate with key KEY in the INDEXER.
    fn delete(&self, key: Vec<u8>) -> bool;
}
