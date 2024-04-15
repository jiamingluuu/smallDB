use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::bitcask::{
    data::log_record::LogRecord, 
    error::Result, 
    fio::IOManager
};

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub const INITIAL_FILE_ID: u32 = 1;

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,          /* An unique identifier to distinguish data files. */
    write_ofs: Arc<RwLock<u64>>,        /* Writing offset, preserved for merging. */
    io_manager: Box<dyn IOManager>,     /* Inerface used for data file rw. */
}

impl DataFile {
    pub fn new(dir_path: &PathBuf, file_id: u32) -> Result<DataFile> {
        todo!()
    }

    pub fn get_write_ofs(&self) -> u64 {
        *self.write_ofs.read().unwrap()
    }
    
    pub fn set_write_ofs(&self, ofs: u64) {
        let mut write_ofs = self.write_ofs.write().unwrap();
        *write_ofs = ofs;
    }

    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read().unwrap()
    }

    pub fn read_log_record(&self, ofs: u64) -> Result<(LogRecord, u64)> {
        todo!()
    }

    pub fn read(&self, buf: &mut [u8], ofs: usize) -> Result<usize> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
