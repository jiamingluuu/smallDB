use std::result;

pub enum Errors {}

pub type Result<T> = result::Result<T, StorageErrors>;

pub enum StorageErrors {
    FailedToReadFromDataFile,
    FailedToWriteToDataFile,
    FailedToSyncToDataFile,
    FailedToOpenDataFile,
}
