use std::result;

pub type Result<T> = result::Result<T, Errors>;

#[derive(Debug, PartialEq)]
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
    InvalidLogRecordCRC,
    ReadDataFileEOF,
    ReadDataFileFailed,
    ExceedMaxBatchNum,
    MergeInProgress,
    UnableToUseWriteBatch,
    DatabaseInUse,
}
