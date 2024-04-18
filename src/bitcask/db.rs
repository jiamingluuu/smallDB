use bytes::Bytes;
use log::warn;
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::bitcask::{
    data::{data_file::*, log_record::*},
    errors::{Errors, Result},
    index::{new_indexer, Indexer},
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

        data_files.reverse();
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
            return Ok(());
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

        // write to the current active file.
        let write_ofs = active_file.get_write_ofs();
        active_file.write(&encoded_record)?;
        if self.options.sync_writes {
            active_file.sync()?;
        }

        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            ofs: write_ofs,
        })
    }

    /// Indexing all the data files.
    fn load_index_from_data_files(&self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(());
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
                    }
                };

                let (log_record, size) = match log_record_res {
                    Ok(result) => result,
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
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
                    LogRecordType::Normal => {
                        self.index.put(log_record.key.to_vec(), log_record_pos)
                    }
                    LogRecordType::Deleted => self.index.delete(log_record.key.to_vec()),
                };

                if !ok {
                    return Err(Errors::IndexUpdateFailed);
                }

                ofs += size as u64;
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
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            let file_name_ = entry.file_name();
            let file_name = file_name_.to_str().unwrap();
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let file_id = file_name
                    .split_once(".")
                    .unwrap()
                    .0
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

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use bytes::Bytes;

    use crate::{
        bitcask::db::Engine,
        bitcask::errors::Errors,
        bitcask::options::Options,
        bitcask::utils::rand_kv::{get_test_key, get_test_value},
    };

    #[test]
    fn test_engine_reboot() {
        let mut opt = Options::default();
        opt.dir_path = PathBuf::from("/tmp/bitkv-rs-reboot");
        let engine = Engine::open(opt.clone()).expect("fail to open engine");

        let res1 = engine.put(get_test_key(11), get_test_value(11));
        assert!(res1.is_ok());
        let res2 = engine.get(get_test_key(11));
        assert!(res2.is_ok());
        assert!(res2.unwrap().len() > 0);

        // restart engine and write data
        std::mem::drop(engine);

        let _engine2 = Engine::open(opt.clone()).expect("fail to reboot engine");
        std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_engine_put() {
        let mut opt = Options::default();
        opt.dir_path = PathBuf::from("/tmp/bitkv-rs-put");
        opt.data_file_size = 64 * 1024 * 1024; // 64MB
        let engine = Engine::open(opt.clone()).expect("fail to open engine");

        // put one item
        let res1 = engine.put(get_test_key(11), get_test_value(11));
        assert!(res1.is_ok());
        let res2 = engine.get(get_test_key(11));
        assert!(res2.is_ok());
        assert!(res2.unwrap().len() > 0);

        // put another item repeatedly
        let res3 = engine.put(get_test_key(22), get_test_value(11));
        assert!(res3.is_ok());
        let res4 = engine.put(get_test_key(22), Bytes::from("11"));
        assert!(res4.is_ok());
        let res5 = engine.get(get_test_key(22));
        assert!(res5.is_ok());
        assert_eq!(res5.unwrap(), Bytes::from("11"));

        // key is empty
        let res6 = engine.put(Bytes::new(), get_test_value(111));
        assert_eq!(Errors::KeyIsEmpty, res6.err().unwrap());

        // value is empty
        let res7 = engine.put(get_test_key(31), Bytes::new());
        assert!(res7.is_ok());
        let res8 = engine.get(get_test_key(31));
        assert_eq!(0, res8.ok().unwrap().len());

        // write to changed data file
        for i in 0..=10000 {
            let res = engine.put(get_test_key(i), get_test_value(i));
            assert!(res.is_ok());
        }

        // restart engine and write data
        std::mem::drop(engine);

        let engine2 = Engine::open(opt.clone()).expect("fail to open engine");
        let res9 = engine2.put(get_test_key(100), get_test_value(100));
        assert!(res9.is_ok());

        let res10 = engine2.get(get_test_key(100));
        assert_eq!(res10.unwrap(), get_test_value(100));

        // delete tested files
        std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_engine_get() {
        let mut opt = Options::default();
        opt.dir_path = PathBuf::from("/tmp/bitkv-rs-get");
        opt.data_file_size = 64 * 1024 * 1024; // 64MB
        let engine = Engine::open(opt.clone()).expect("fail to open engine");

        // read one item
        let res1 = engine.put(get_test_key(11), get_test_value(11));
        assert!(res1.is_ok());
        let res2 = engine.get(get_test_key(11));
        assert!(res2.is_ok());
        assert!(res2.unwrap().len() > 0);

        // read after putting another items
        let res3 = engine.put(get_test_key(22), Bytes::from("22"));
        assert!(res3.is_ok());
        let res4 = engine.put(get_test_key(33), get_test_value(33));
        assert!(res4.is_ok());
        let res5 = engine.get(get_test_key(22));
        assert!(res5.is_ok());
        assert_eq!(res5.unwrap(), Bytes::from("22"));

        // read when key is invaild
        let res6 = engine.get(Bytes::from("not exist"));
        assert_eq!(Errors::KeyNotFound, res6.err().unwrap());

        // read after value is deleted
        let res7 = engine.put(get_test_key(31), Bytes::new());
        assert!(res7.is_ok());
        let res8 = engine.delete(get_test_key(31));
        assert!(res8.is_ok());
        let res9 = engine.get(get_test_key(31));
        assert_eq!(Errors::KeyNotFound, res9.err().unwrap());

        // read from old data file
        for i in 500..=100000 {
            let res = engine.put(get_test_key(i), get_test_value(i));
            assert!(res.is_ok());
        }
        let res10 = engine.get(get_test_key(5000));
        assert!(res10.is_ok());

        // restart engine and read data
        std::mem::drop(engine);

        let engine2 = Engine::open(opt.clone()).expect("fail to open engine");
        let res11 = engine2.get(get_test_key(33));
        assert_eq!(get_test_value(33), res11.unwrap());

        let res12 = engine2.get(get_test_key(22));
        assert_eq!(Bytes::from("22"), res12.unwrap());

        let res13 = engine2.get(get_test_key(333));
        assert_eq!(Errors::KeyNotFound, res13.err().unwrap());

        // delete tested files
        std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_engine_delete() {
        let mut opt = Options::default();
        opt.dir_path = PathBuf::from("/tmp/bitkv-rs-delete");
        opt.data_file_size = 64 * 1024 * 1024; // 64MB
        let engine = Engine::open(opt.clone()).expect("fail to open engine");

        // delete one item
        let res1 = engine.put(get_test_key(11), Bytes::new());
        assert!(res1.is_ok());
        let res2 = engine.delete(get_test_key(11));
        assert!(res2.is_ok());
        let res3 = engine.get(get_test_key(11));
        assert_eq!(Errors::KeyNotFound, res3.err().unwrap());

        // delete a non-exist item
        let res4 = engine.delete(Bytes::from("not existed key"));
        assert!(res4.is_ok());

        // delete an empty key
        let res5 = engine.delete(Bytes::new());
        assert_eq!(Errors::KeyIsEmpty, res5.err().unwrap());

        // delete and put again
        let res6 = engine.put(get_test_key(11), get_test_value(11));
        assert!(res6.is_ok());
        let res7 = engine.delete(get_test_key(11));
        assert!(res7.is_ok());
        let res8 = engine.put(get_test_key(11), get_test_value(11));
        assert!(res8.is_ok());
        let res9 = engine.get(get_test_key(11));
        assert!(res9.is_ok());

        // restart engine and delete data
        std::mem::drop(engine);
        let engine2 = Engine::open(opt.clone()).expect("fail to open engine");
        let res10 = engine2.delete(get_test_key(11));
        assert!(res10.is_ok());
        let res11 = engine2.get(get_test_key(11));
        assert_eq!(Errors::KeyNotFound, res11.err().unwrap());

        // delete tested files
        std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
    }
}
