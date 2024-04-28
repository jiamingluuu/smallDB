pub mod file_io;

use std::path::PathBuf;

use crate::bitcask::errors::Result;

use self::file_io::FileIO;

/// IO managing abstraction.
pub trait IOManager: Sync + Send {
    /// Read from file SELF starting at offset OFS to the buffer BUF.
    fn read(&self, buf: &mut [u8], ofs: u64) -> Result<usize>;

    /// Write to file SELF with content in BUF.
    fn write(&self, buf: &[u8]) -> Result<usize>;

    /// Persist data
    fn sync(&self) -> Result<()>;
}

/// Initialize IOMANAGER according to the file type.
pub fn new_io_manager(file_name: PathBuf) -> Result<impl IOManager> {
    FileIO::new(file_name)
}
