use bytes::Bytes;
use std::{collections::HashMap, sync::Arc, sync::RwLock};

use crate::bitcask::{
    data::{data_file::DataFile, log_record::*},
    error::{Result, StorageErrors},
    index,
    options::Options,
};

/// struct used for storage, the running instance of Bitcask
pub struct Engine {
    options: Arc<Options>, /* The configuration for the database */
    active_file: Arc<RwLock<DataFile>>, /* A file is active only if it is writing by
                           the server. */
    old_files: Arc<RwLock<HashMap<u32, DataFile>>>, /* The keydir. */
    index: Box<dyn index::Indexer>,
}

impl Engine {
    /// Write key KEY with value VALUE into the database
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(StorageErrors::KeyIsEmpty);
        }

        let mut log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::Normal,
        };

        // Update the location of newest data.
        let log_record_pos = self.append_log_record(&mut log_record)?;
        if !self.index.put(key.to_vec(), log_record_pos) {
            return Err(StorageErrors::IndexUpdateFailed);
        }

        Ok(())
    }

    /// Get the data with key KEY from the database
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(StorageErrors::KeyIsEmpty);
        }

        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(StorageErrors::KeyNotFound);
        }

        // If the record position is at active file, read it from active file, otherwise read it
        // from old files.
        let log_record_pos = pos.unwrap();
        let active_file = self.active_file.read().unwrap();
        let old_files = self.old_files.read().unwrap();
        let log_record = if active_file.get_file_id() == log_record_pos.file_id {
            active_file.read_log_record(log_record_pos.ofs)?
        } else {
            match old_files.get(&log_record_pos.file_id) {
                None => return Err(StorageErrors::DataFileNotFound),
                Some(data_file) => data_file.read_log_record(log_record_pos.ofs)?,
            }
        };

        // Check if the current record has been deleted
        if log_record.record_type == LogRecordType::Deleted {
            return Err(StorageErrors::KeyNotFound);
        }

        Ok(log_record.value.into())
    }

    /// Write to the active file by appending.
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();

        let encoded_record = log_record.encode();
        let record_len = encoded_record.len() as u64;

        let mut active_file = self.active_file.write().unwrap();

        // When the current active file meets a size threshold, close it and
        // create a new active file.
        if active_file.get_write_ofs() + record_len > self.options.data_file_size {
            // Persist the current active file to the disk.
            active_file.sync()?;
            let file_id = active_file.get_file_id();

            // Close the current active file, and insert it into the keydir.
            let mut old_files = self.old_files.write().unwrap();
            let old_file = DataFile::new(dir_path.clone(), file_id)?;
            old_files.insert(file_id, old_file);

            // Create a new active file.
            let new_file = DataFile::new(dir_path.clone(), file_id + 1)?;
            *active_file = new_file;
        }

        active_file.write(&encoded_record)?;
        if self.options.sync_writes {
            active_file.sync()?;
        }

        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            ofs: active_file.get_write_ofs(),
        })
    }
}
