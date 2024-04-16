use std::path::PathBuf;

/// The configuration for database.
#[derive(Clone)]
pub struct Options {
    pub dir_path: PathBuf,      /* The location of keydir */
    pub data_file_size: u64,    /* The threshold for active file size. Close
                                   the current file if exceed this threshold. */
    pub sync_writes: bool,      /* Synchronize the writing. */
    pub index_type: IndexType,  /* The data structure used for indexer */
}

#[derive(Clone)]
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
            sync_writes: false,
            index_type: IndexType::BTree,
        }
    }
}
