use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::{
    errors::{Errors, Result},
    fio::IOManager,
};

pub struct FileIO {
    pub(crate) file: Arc<RwLock<File>>,
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
            Ok(file_) => Ok(FileIO {
                file: Arc::new(RwLock::new(file_)),
            }),
            Err(e) => {
                eprintln!("[FileIO: new] Failed to open data file, {}", e);
                Err(Errors::FailedToOpenDataFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], ofs: u64) -> Result<usize> {
        let file = self.file.read().unwrap();
        file.read_at(buf, ofs)
            .map_err(|_| Errors::FailedToOpenDataFile)
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut file = self.file.write().unwrap();
        file.write(buf).map_err(|_| Errors::FailedToWriteToDataFile)
    }

    fn sync(&self) -> Result<()> {
        let file = self.file.read().unwrap();
        file.sync_all().map_err(|_| Errors::FailedToSyncToDataFile)
    }

    fn size(&self) -> u64 {
        let file = self.file.read().unwrap();
        file.metadata().unwrap().len()
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

        let res1 = fio.write(b"hello ");
        assert!(res1.is_ok());
        assert_eq!(6, res1.ok().unwrap());

        let res2 = fio.write(b"world");
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        assert!(std::fs::remove_file(path.clone()).is_ok());
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/b.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());

        let fio = fio_res.ok().unwrap();

        let w1 = fio.write("hello ".as_bytes());
        assert!(w1.is_ok());
        assert_eq!(6, w1.ok().unwrap());

        let mut buf = [0 as u8; 100];
        let mut r = fio.read(&mut buf, 0);
        assert!(r.is_ok());
        assert_eq!(r.ok().unwrap(), 6);
        let mut slice_pos = buf.iter().position(|&x| x == 0).unwrap();
        assert_eq!(&buf[..slice_pos], b"hello ");

        let w2 = fio.write(b"world");
        assert!(w2.is_ok());
        assert_eq!(5, w2.ok().unwrap());
        r = fio.read(&mut buf, 0);
        assert!(r.is_ok());
        assert_eq!(r.ok().unwrap(), 11);
        slice_pos = buf.iter().position(|&x| x == 0).unwrap();
        assert_eq!(&buf[..slice_pos], b"hello world");

        assert!(std::fs::remove_file(path.clone()).is_ok());
    }

    #[test]
    fn test_file_io_sync() {
        let path = PathBuf::from("/tmp/c.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());

        let fio = fio_res.ok().unwrap();

        let res1 = fio.write(b"hello ");
        assert!(res1.is_ok());
        assert_eq!(6, res1.ok().unwrap());

        let res2 = fio.write(b"world");
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        let sync_res = fio.sync();
        assert!(sync_res.is_ok());

        assert!(std::fs::remove_file(path.clone()).is_ok());
    }
}
