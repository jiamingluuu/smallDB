use bytes::{Buf, BytesMut};
use prost::{decode_length_delimiter, length_delimiter_len};

use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::bitcask::{
    data::log_record::{max_log_record_header_size, LogRecord, LogRecordType},
    error::{Errors, Result},
    fio::{new_io_manager, IOManager},
};

/// Convention: All bitcask data files are end with .DATA.
pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub const INITIAL_FILE_ID: u32 = 1;
const TYPE_SIZE: usize = 1;
const CRC_LEN: usize = 4;

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,      /* An unique identifier to distinguish data files. */
    write_ofs: Arc<RwLock<u64>>,    /* Writing offset, preserved for merging. */
    io_manager: Box<dyn IOManager>, /* Interface used for data file read and write. */
}

impl DataFile {
    /// Initialize a new DataFile struct according to DIR_PATH and FILE_ID.
    pub fn new(dir_path: &PathBuf, file_id: u32) -> Result<DataFile> {
        let file_name = get_data_file_name(dir_path, file_id);
        let io_manager = new_io_manager(file_name)?;
        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_ofs: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
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

    // Read the log record from
    pub fn read_log_record(&self, ofs: u64) -> Result<(LogRecord, usize)> {
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header_buf, ofs)?;

        let record_type = LogRecordType::from_u8(header_buf.get_u8());
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();

        // If there were no key, nor value, it is indicating we reach the end of file.
        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEOF);
        }

        // HEADER_SIZE = 1 bytes for type + len(key_size) + len(value_size)
        let header_size =
            TYPE_SIZE + length_delimiter_len(key_size) + length_delimiter_len(value_size);

        let mut kv_buf = BytesMut::zeroed(key_size + value_size + CRC_LEN);
        self.io_manager
            .read(&mut kv_buf, ofs + header_size as u64)?;
        let mut log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            record_type,
        };

        // Check for CRC.
        kv_buf.advance(key_size + value_size);
        if kv_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::InvalidLogRecordCRC);
        }

        Ok((log_record, header_size + key_size + value_size + 4))
    }

    pub fn read(&self, buf: &mut [u8], ofs: usize) -> Result<usize> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let size = self.io_manager.write(buf)?;
        *self.write_ofs.write().unwrap() += size as u64;
        Ok(size)
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}

fn get_data_file_name(dir_path: &PathBuf, file_id: u32) -> PathBuf {
    let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    dir_path.join(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_date_file() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(&dir_path, 0);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);

        let data_file_res2 = DataFile::new(&dir_path, 0);
        assert!(data_file_res2.is_ok());
        let data_file2 = data_file_res2.unwrap();
        assert_eq!(data_file2.get_file_id(), 0);

        let data_file_res3 = DataFile::new(&dir_path, 123);
        assert!(data_file_res3.is_ok());
        let data_file3 = data_file_res3.unwrap();
        assert_eq!(data_file3.get_file_id(), 123);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(&dir_path, 0);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);

        let write_res1 = data_file1.write("to be or not to be".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), "to be or not to be".len());

        let write_res2 = data_file1.write("that is a question".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), "that is a question".len());
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(&dir_path, 0);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);

        let sync_res = data_file1.sync();
        assert!(sync_res.is_ok());
    }
}
