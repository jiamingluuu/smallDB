use std::{
    fs::OpenOptions,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use memmap2::Mmap;

use crate::bitcask::errors::{Errors, Result};

use super::IOManager;

pub struct MMapIO {
    map: Arc<Mutex<Mmap>>,
}

impl MMapIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new().create(true).read(true).write(true).open(file_name) {
            Ok(file) => Ok(MMapIO {
                map: Arc::new(Mutex::new(unsafe {
                    Mmap::map(&file).expect("failed to map the file")
                }))
            }),
            Err(e) => {
                eprintln!("[FileIO: new] Failed to open data file, {}", e);
                Err(Errors::FailedToOpenDataFile)
            }
        }
    }
}

impl IOManager for MMapIO {
    fn read(&self, buf: &mut [u8], ofs: u64) -> Result<usize> {
        let map = self.map.lock().unwrap();
        let end = ofs + buf.len() as u64;
        if end > map.len() as u64 {
            return Err(Errors::ReadDataFileEOF);
        }
        let val = &map[ofs as usize..end as usize];
        buf.copy_from_slice(val);

        Ok(val.len())
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    fn size(&self) -> u64 {
        self.map.lock().unwrap().len() as u64
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::bitcask::fio::file_io::FileIO;

    use super::*;

    #[test]
    fn test_mmap_read() {
        let path = PathBuf::from("/tmp/mmap-test.data");

        let mmap_res1 = MMapIO::new(path.clone());
        assert!(mmap_res1.is_ok());
        let mmap_io1 = mmap_res1.ok().unwrap();
        let mut buf1 = [0u8; 10];
        let read_res1 = mmap_io1.read(&mut buf1, 0);
        assert_eq!(read_res1.err().unwrap(), Errors::ReadDataFileEOF);

        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();
        fio.write(b"aa").unwrap();
        fio.write(b"bb").unwrap();
        fio.write(b"cc").unwrap();

        let mmap_res2 = MMapIO::new(path.clone());
        assert!(mmap_res2.is_ok());
        let mmap_io2 = mmap_res2.ok().unwrap();

        let mut buf2 = [0u8; 2];
        let read_res2 = mmap_io2.read(&mut buf2, 2);
        assert!(read_res2.is_ok());

        let remove_res = fs::remove_file(path.clone());
        assert!(remove_res.is_ok());
    }
}
