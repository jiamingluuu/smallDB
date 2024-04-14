use std::result;

pub type Result<T> = result::Result<T, StorageErrors>;

pub enum StorageErrors {
    DataFileNotFound,
    FailedToReadFromDataFile,
    FailedToWriteToDataFile,
    FailedToSyncToDataFile,
    FailedToOpenDataFile,
    KeyIsEmpty,
    KeyNotFound,
    IndexUpdateFailed,
}
