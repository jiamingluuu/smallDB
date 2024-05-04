use std::path::PathBuf;

/// The configuration for database, where:
/// - `dir_path` is the location of key directory.
/// - `data_file_size` determines the threshold for active file size. The active data file is
///     closed when if it exceeds this threshold.
/// - `bytes_per_sync` determines when should we perform a synchronization of data.
/// - `sync_writes` ensures the data sync persistence on writing if set to TRUE.
/// - `index_type` determines the indexer used for storage.
#[derive(Clone)]
pub struct Options {
    pub dir_path: PathBuf,
    pub data_file_size: u64,
    pub bytes_per_sync: usize,
    pub sync_writes: bool,
    pub index_type: IndexType,
    pub startup_io_type: IOType,
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
            startup_io_type: IOType::StandaradFIO,
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
    StandaradFIO,
    MemoryMapped,
}
