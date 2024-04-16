use bytes::Bytes;
use log::warn;
use std::{
    collections::HashMap, 
    fs, 
    path::PathBuf, 
    sync::{Arc, RwLock}
};

use crate::bitcask::{
    data::{data_file::*, log_record::*},
    error::{Result, Errors},
    index::{Indexer, new_indexer},
    options::Options,
};

/// struct used for storage, the running instance of Bitcask
pub struct Engine {
    options: Arc<Options>,                          /* The configuration for the database */
    active_file: Arc<RwLock<DataFile>>,             /* A file is active only if it is writing by
                                                       the server. */
    old_files: Arc<RwLock<HashMap<u32, DataFile>>>, /* The keydir. */
    index: Box<dyn Indexer>,                        /* Indexer used for cache. */
    file_ids: Vec<u32>, 
}

impl Engine {
    /// Open a bitcast instance with configuration OPTS.
    pub fn open(opts: Options) -> Result<Self> {
        if let Some(e) = check_options(&opts) {
            return Err(e);
        }

        let options = opts.clone();
        let dir_path = opts.dir_path.clone();
        if !dir_path.is_dir() {
            if let Err(e) = fs::create_dir_all(dir_path.clone()) {
                warn!("create database directory error {}", e);
                return Err(Errors::FailedToSyncToDataFile);
            }
        }

        let mut data_files = load_data_files(dir_path.clone())?;
        let file_ids: Vec<u32> = data_files
            .iter()
            .map(|data_file| data_file.get_file_id())
            .collect();

        // The last file is the active file, and the rest are old files.
        let mut old_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let data_file = data_files.pop().unwrap();
                old_files.insert(data_file.get_file_id(), data_file);
            }
        };

        let active_file = match data_files.pop() {
            Some(v) => v,
            // It is possible to have an empty directory, in this case we create a NULL active
            // file.
            None => DataFile::new(&dir_path, INITIAL_FILE_ID)?,
        };

        let engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            old_files: Arc::new(RwLock::new(old_files)),
            index: Box::new(new_indexer(options.index_type)),
            file_ids,
        };

        engine.load_index_from_data_files()?;

        Ok(engine)
    }

    /// Write the pair (KEY, VALUE) into the database
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let mut log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::Normal,
        };

        // Update the location of newest data.
        let log_record_pos = self.append_log_record(&mut log_record)?;
        if !self.index.put(key.to_vec(), log_record_pos) {
            return Err(Errors::IndexUpdateFailed);
        }

        Ok(())
    }

    /// Delete the entry with key KEY.
    pub fn delete(&self, key: Bytes) -> Result<()> {
        // On merging, bitcask will write the newest log record to the disk. Hence, in order to
        // delete a record, all we need to do is just append the record with an entry that has the
        // same key but with the tombstone set to DELETED.
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(())
        }

        let mut log_record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            record_type: LogRecordType::Deleted,
        };

        self.append_log_record(&mut log_record)?;
        if !self.index.delete(key.to_vec()) {
            return Err(Errors::IndexUpdateFailed);
        }

        Ok(())
    }

    /// Get the data with key KEY from the database
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(Errors::KeyNotFound);
        }

        // If the record position is at active file, read it from active file, otherwise read it
        // from old files.
        let log_record_pos = pos.unwrap();
        let active_file = self.active_file.read().unwrap();
        let old_files = self.old_files.read().unwrap();
        let (log_record, _) = if active_file.get_file_id() == log_record_pos.file_id {
            active_file.read_log_record(log_record_pos.ofs)?
        } else {
            match old_files.get(&log_record_pos.file_id) {
                None => return Err(Errors::DataFileNotFound),
                Some(data_file) => data_file.read_log_record(log_record_pos.ofs)?,
            }
        };

        // Check if the current record has been deleted
        if log_record.record_type == LogRecordType::Deleted {
            return Err(Errors::KeyNotFound);
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
            let old_file = DataFile::new(&dir_path, file_id)?;
            old_files.insert(file_id, old_file);

            // Create a new active file.
            let new_file = DataFile::new(&dir_path, file_id + 1)?;
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

    /// Indexing all the data files.
    fn load_index_from_data_files(&self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(())
        }

        let active_file = self.active_file.read().unwrap();
        let old_files = self.old_files.read().unwrap();

        for (i, file_id) in self.file_ids.iter().enumerate() {
            // continuous read the file with id FILE_ID
            let mut ofs = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(ofs),
                    false => {
                        let data_file = old_files.get(file_id).unwrap();
                        data_file.read_log_record(ofs)
                    },
                };

                let (log_record, size) = match log_record_res {
                    Ok(result) => result,
                    Err(e) => {
                        if e == Errors::ReadDataFileFailed {
                            // This case indicates all content within the current file has been
                            // read. Therefore, we break the current loop and read the next file.
                            break;
                        } else {
                            return Err(e);
                        }
                    }
                };

                // Load LOG_RECORD to cache.
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    ofs,
                };

                let ok = match log_record.record_type {
                    LogRecordType::Normal => self.index.put(log_record.key.to_vec(), log_record_pos),
                    LogRecordType::Deleted => self.index.delete(log_record.key.to_vec()),
                };

                if !ok {
                    return Err(Errors::IndexUpdateFailed);
                }

                ofs += size;
            }

            if i == self.file_ids.len() - 1 {
                active_file.set_write_ofs(ofs)
            }
        }

        Ok(())
    }
}

/// Fetch all data files under directory DIR_PATH.
fn load_data_files(dir_path: PathBuf) -> Result<Vec<DataFile>> {
    let dir = fs::read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(Errors::FailedToReadDatabaseDir);
    }

    let mut file_ids = Vec::<u32>::new();
    let mut data_files = Vec::<DataFile>::new();
    for file in dir.unwrap(){
        if let Ok(entry) = file {
            let file_name_ = entry.file_name();
            let file_name = file_name_.to_str().unwrap();
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let file_id = file_name
                    .split_once(".")
                    .unwrap().0
                    .parse::<u32>()
                    .map_err(|_| Errors::DataDirectoryCorrupted)?;
                file_ids.push(file_id);
            }
        }
    }

    file_ids.sort();
    for file_id in file_ids {
        data_files.push(DataFile::new(&dir_path, file_id)?);
    }

    Ok(data_files)
}

fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(Errors::DirPathIsEmpty);
    }

    if opts.data_file_size <= 0 {
        return Some(Errors::DataFileSizeTooSmall);
    }

    None
}
