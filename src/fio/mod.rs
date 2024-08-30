pub mod file_io;
pub mod mmap;

use std::path::PathBuf;

use crate::errors::Result;

use self::{file_io::FileIO, mmap::MMapIO};

use super::options::IOType;

/// IO managing abstraction.
pub trait IOManager: Sync + Send {
    /// Read from file SELF starting at offset OFS to the buffer BUF.
    fn read(&self, buf: &mut [u8], ofs: u64) -> Result<usize>;

    /// Write to file SELF with content in BUF.
    fn write(&self, buf: &[u8]) -> Result<usize>;

    /// Synchronize data.
    fn sync(&self) -> Result<()>;

    /// Get the size of current data file.
    fn size(&self) -> u64;
}

/// Initialize IOMANAGER according to the file type.
pub fn new_io_manager(file_name: PathBuf, io_type: IOType) -> Box<dyn IOManager> {
    match io_type {
        IOType::StandardFIO => Box::new(FileIO::new(file_name).unwrap()),
        IOType::MemoryMapped => Box::new(MMapIO::new(file_name).unwrap()),
    }
}
