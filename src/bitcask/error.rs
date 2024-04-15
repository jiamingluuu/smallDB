use std::result;

pub type Result<T> = result::Result<T, Errors>;

#[derive(PartialEq)]
pub enum Errors {
    DataFileNotFound,
    DirPathIsEmpty,
    DataFileSizeTooSmall,
    DataDirectoryCorrupted,
    FailedToReadFromDataFile,
    FailedToWriteToDataFile,
    FailedToSyncToDataFile,
    FailedToOpenDataFile,
    FailedToCreateDatabaseDir,
    FailedToReadDatabaseDir,
    KeyIsEmpty,
    KeyNotFound,
    IndexUpdateFailed,
    ReadDataFileFailed,
}
