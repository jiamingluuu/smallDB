pub mod btree;

use bytes::Bytes;

use crate::bitcask::{
    data::log_record::LogRecordPos,
    errors::Result,
    options::{IndexType, IteratorOptions},
};

/// Interface for data indexing abstraction.
pub trait Indexer: Sync + Send {
    /// Write KEY to INDEXER at position POS.
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;

    /// Read KEY from INDEXER.
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// Delete the index associate with key KEY in the INDEXER.
    fn delete(&self, key: Vec<u8>) -> bool;

    /// Get all keys contained in the engine.
    fn list_keys(&self) -> Result<Vec<Bytes>>;

    /// Get the index iterator.
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}

pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BPTree => todo!(),
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => todo!(),
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
