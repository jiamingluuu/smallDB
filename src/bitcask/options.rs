use std::path::PathBuf;

/// The configuration for database.
pub struct Options {
    pub dir_path: PathBuf, /* The location of keydir */
    pub data_file_size: u64, /* The threshold for active file size. Close
                           the current file if exceed this threshold. */
    pub sync_writes: bool, /* Synchronize the writing. */
}
