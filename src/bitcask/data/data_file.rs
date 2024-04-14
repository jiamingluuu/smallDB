use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::bitcask::{data::log_record::LogRecord, error::Result, fio::IOManager};

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,   // file_id
    write_ofs: Arc<RwLock<u64>>, // Writing offset, preserved for merging
    io_manager: Box<dyn IOManager>,
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        todo!()
    }

    pub fn get_write_ofs(&self) -> u64 {
        *self.write_ofs.read().unwrap()
    }

    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read().unwrap()
    }

    pub fn read(&self, buf: &mut [u8], ofs: usize) -> Result<usize> {
        todo!()
    }

    pub fn read_log_record(&self, ofs: u64) -> Result<LogRecord> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
