use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::bitcask::error::{Result, StorageErrors};
use crate::bitcask::fio::IOManager;

pub struct FileIO {
    pub(crate) fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => Ok(FileIO {
                fd: Arc::new(RwLock::new(file)),
            }),
            Err(e) => {
                eprintln!("[FileIO: new] Failed to open data file, {}", e);
                Err(StorageErrors::FailedToOpenDataFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], ofs: u64) -> Result<usize> {
        let read_guard = self.fd.read().unwrap();
        match read_guard.read_at(buf, ofs) {
            Ok(byte_count) => Ok(byte_count),
            Err(e) => {
                eprintln!("[FileIO: read] Failed to read from data file, {}", e);
                Err(StorageErrors::FailedToReadFromDataFile)
            }
        }
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write().unwrap();
        match write_guard.write(buf) {
            Ok(byte_count) => Ok(byte_count),
            Err(e) => {
                eprintln!("[FileIO: write] Failed to write to data file, {}", e);
                Err(StorageErrors::FailedToWriteToDataFile)
            }
        }
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read().unwrap();
        match read_guard.sync_all() {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("[FileIO: sync] Failed to sync to data file {}", e);
                Err(StorageErrors::FailedToSyncToDataFile)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from("/tmp/a.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());

        let fio = fio_res.ok().unwrap();

        let res1 = fio.write("hello ".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(6, res1.ok().unwrap());

        let res2 = fio.write("world".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        assert!(std::fs::remove_file(path.clone()).is_ok());
    }
}
