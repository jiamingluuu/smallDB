use std::path::PathBuf;

/// The configuration for database, where:
#[derive(Clone)]
pub struct Options {
    /// The location of key directory.
    pub dir_path: PathBuf,

    /// The threshold for active file size. The active data file is closed when if it exceeds this threshold.
    pub data_file_size: u64,

    /// The threshold of performing a synchronization of data.
    pub bytes_per_sync: usize,

    /// The data persist to disk for every writing if set to TRUE.
    pub sync_writes: bool,

    /// Determines the indexer used for storage.
    pub index_type: IndexType,

    /// The IO type used for starting the engine.
    pub startup_io_type: IOType,
    
    /// Threshold for performing merge process.
    pub data_file_merge_ratio: f32,
}

#[derive(Clone, PartialEq)]
pub enum IndexType {
    BPTree,
    BTree,
    SkipList,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-data"),
            data_file_size: 256 * 1024 * 1024,
            bytes_per_sync: 0,
            sync_writes: false,
            index_type: IndexType::BTree,
            startup_io_type: IOType::StandardFIO,
            data_file_merge_ratio: 0.5,
        }
    }
}

/// The configuration for iterator.
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

/// The configuration for writing, where:
/// - `max_batch_num` determines the maximum number of write per batch.
/// - `sync_writes` ensures the data sync persistence on writing if set to TRUE.
pub struct WriteBatchOptions {
    pub max_batch_num: usize,
    pub sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 10000,
            sync_writes: true,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum IOType {
    StandardFIO,
    MemoryMapped,
}
