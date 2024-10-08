pub mod bptree;
pub mod btree;
pub mod skiplist;

use std::path::PathBuf;

use bytes::Bytes;

use crate::{
    data::log_record::LogRecordPos,
    errors::Result,
    options::{IndexType, IteratorOptions},
};

/// Interface for data indexing abstraction.
pub trait Indexer: Sync + Send {
    /// Write KEY to INDEXER at position POS.
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos>;

    /// Read KEY from INDEXER.
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// Delete the index associate with key KEY in the INDEXER.
    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// Get all keys contained in the engine.
    fn list_keys(&self) -> Result<Vec<Bytes>>;

    /// Get the index iterator.
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}

pub fn new_indexer(index_type: IndexType, dir_path: PathBuf) -> Box<dyn Indexer> {
    match index_type {
        IndexType::BTree => Box::new(btree::BTree::new()),
        IndexType::BPTree => Box::new(bptree::BPTree::new(dir_path)),
        IndexType::SkipList => Box::new(skiplist::SkipList::new()),
    }
}

/// Interface for indexer iterator.
pub trait IndexIterator: Sync + Send {
    /// Start the iterator to the beginning of all items.
    fn rewind(&mut self);

    /// Start the iterator to the first item with key that is greater or equal to KEY.
    fn seek(&mut self, key: Vec<u8>);

    /// Go to the next item of the iterator.
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
