mod file_io;

use crate::bitcask::error::Result;

/// IO managing abstraction.
pub trait IOManager: Sync + Send {
    /// Read from file SELF starting at offset OFS to the buffer BUF.
    fn read(&self, buf: &mut [u8], ofs: u64) -> Result<usize>;

    /// Write to file SELF with content in BUF.
    fn write(&self, buf: &[u8]) -> Result<usize>;

    /// Persist data
    fn sync(&self) -> Result<()>;
}
