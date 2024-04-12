use std::result;

pub type Result<T> = result::Result<T, StorageErrors>;

pub enum StorageErrors {
    FailedToReadFromDataFile,
    FailedToWriteToDataFile,
    FailedToSyncToDataFile,
    FailedToOpenDataFile,
}
