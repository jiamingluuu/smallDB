use bytes::{BufMut, Bytes, BytesMut};
use log::warn;
use prost::{decode_length_delimiter, encode_length_delimiter};
use std::{
    collections::HashMap, fs, path::PathBuf, sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    }
};

use super::{
    data::{data_file::*, log_record::*},
    errors::{Errors, Result},
    index::{new_indexer, Indexer},
    options::Options,
    batch::NON_TRANSACTION_SEQUENCE, 
    merge::load_merge_files, 
    options::IndexType,
};


const INITIAL_FILE_ID: u32 = 1;
const SEQUENCE_NUMBER_KEY: &str = "seq-no";

/// struct used for storage, the running instance of Bitcask, where
/// - `options` is the configuration for the database engine.
/// - `active_file` indicates the current file that is used for storing all log record.
/// - `old_files` stores all the closed data file, also called keydir.
/// - `index` provides an interface for data file indexing.
/// - `file_ids` collects all the data file id.
/// - `batch_commit_lock` prevents race conditions while committing transaction.
/// - `sequence_number` is an unique increasing identifier for transaction. 0 indicates the
///     current data file is not committed by a transaction.
/// - `merge_lock` prevents race condition during merge process.
pub struct Engine {
    pub(crate) options: Arc<Options>,
    pub(crate) active_file: Arc<RwLock<DataFile>>,
    pub(crate) old_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    pub(crate) index: Box<dyn Indexer>,
    pub(crate) file_ids: Vec<u32>,
    pub(crate) batch_commit_lock: Mutex<()>,
    pub(crate) sequence_number: Arc<AtomicUsize>,
    pub(crate) merge_lock: Mutex<()>,
    pub(crate) sequence_file_exists: bool,
    pub(crate) is_first_time_init: bool,
}

impl Engine {
    /// Open a bitcast instance with configuration OPTS.
    pub fn open(opts: Options) -> Result<Self> {
        if let Some(e) = check_options(&opts) {
            return Err(e);
        }

        let mut is_first_time_init = false;
        let options = opts.clone();
        let dir_path = opts.dir_path.clone();
        if !dir_path.is_dir() {
            is_first_time_init = true;
            if let Err(e) = fs::create_dir_all(dir_path.clone()) {
                warn!("create database directory error {}", e);
                return Err(Errors::FailedToSyncToDataFile);
            }
        }
        let entries = fs::read_dir(&dir_path).unwrap();
        if entries.count() == 0 {
            is_first_time_init = true;
        }

        load_merge_files(&dir_path)?;

        let mut data_files = load_data_files(&dir_path)?;
        let file_ids: Vec<u32> = data_files
            .iter()
            .map(|data_file| data_file.get_file_id())
            .collect();

        // The last file is the active file, and the rest are old files.
        data_files.reverse();
        let mut old_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let data_file = data_files.pop().unwrap();
                old_files.insert(data_file.get_file_id(), data_file);
            }
        };

        let active_file = match data_files.pop() {
            Some(v) => v,
            // It is possible to have an empty directory, an empty active file is created in this
            // case.
            None => DataFile::new(&dir_path, INITIAL_FILE_ID)?,
        };

        let mut engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            old_files: Arc::new(RwLock::new(old_files)),
            index: new_indexer(options.index_type, options.dir_path),
            file_ids,
            batch_commit_lock: Mutex::new(()),
            sequence_number: Arc::new(AtomicUsize::new(1)), // Initialized to 1 to prevent conflict to NON_TRANSACTION_SEQUENCE
            merge_lock: Mutex::new(()),
            sequence_file_exists: false,
            is_first_time_init,
        };

        match engine.options.index_type {
            IndexType::BTree | IndexType::SkipList => {
                // Load index from hint file to speed up the reboot of bitcask engine after shutdown.
                engine.load_index_from_hint_file()?;

                let current_sequence_number = engine.load_index_from_data_files()?;
                if current_sequence_number > 0 {
                    engine
                        .sequence_number
                        .store(current_sequence_number + 1, Ordering::Relaxed);
                }
            }
            IndexType::BPTree => {
                let (exists, sequence_number) = engine.load_sequence_number();
                engine.sequence_number.store(sequence_number, Ordering::SeqCst);
                engine.sequence_file_exists = exists;
                
                // Set the offset of current active file
                let active_file = engine.active_file.write().unwrap();
                active_file.set_write_ofs(active_file.file_size());
            }
        }

        Ok(engine)
    }
    
    pub fn close(&self) -> Result<()> {
        let sequence_number_file = DataFile::new_sequence_number_file(&self.options.dir_path)?;
        let sequence_number = self.sequence_number.load(Ordering::SeqCst);
        let record = LogRecord {
            key: SEQUENCE_NUMBER_KEY.as_bytes().to_vec(),
            value: sequence_number.to_string().into_bytes(),
            record_type: LogRecordType::Normal,
        };
        sequence_number_file.write(&record.encode())?;
        sequence_number_file.sync()?;

        self.active_file.read().unwrap().sync()
    }

    /// Write the pair (KEY, VALUE) into the database
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let mut log_record = LogRecord {
            key: encode_log_record_key(key.to_vec(), NON_TRANSACTION_SEQUENCE),
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
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }

        let mut log_record = LogRecord {
            key: encode_log_record_key(key.to_vec(), NON_TRANSACTION_SEQUENCE),
            value: Default::default(),
            record_type: LogRecordType::Deleted,
        };

        self.append_log_record(&mut log_record)?;
        if !self.index.delete(key.to_vec()) {
            return Err(Errors::IndexUpdateFailed);
        }

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.active_file.read().unwrap().sync()
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

        self.get_value_by_position(&log_record_pos)
    }

    pub(crate) fn get_value_by_position(&self, log_record_pos: &LogRecordPos) -> Result<Bytes> {
        let active_file = self.active_file.read().unwrap();
        let old_files = self.old_files.read().unwrap();

        // LOG_RECORD_POS may appears in either active file or closed files, so we need to check
        // both of them.
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.ofs)?.0,
            false => {
                let data_file = old_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileNotFound);
                }
                data_file.unwrap().read_log_record(log_record_pos.ofs)?.0
            }
        };

        if log_record.record_type == LogRecordType::Deleted {
            return Err(Errors::KeyNotFound);
        }

        Ok(log_record.value.into())
    }

    /// Write to the active file by appending the file with LOG_RECORD.
    pub(crate) fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();

        let encoded_record = log_record.encode();
        let record_len = encoded_record.len() as u64;

        let mut active_file = self.active_file.write().unwrap();

        // When the current active file meets a size threshold, close it and create a new active
        // file.
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
    fn load_index_from_data_files(&self) -> Result<usize> {
        let mut current_sequence_number = NON_TRANSACTION_SEQUENCE;
        if self.file_ids.is_empty() {
            return Ok(current_sequence_number);
        }

        // Obtain the id of the file that has not been merged.
        let mut has_merge = false;
        let mut non_merge_fid = 0;
        let merge_fin_file = self.options.dir_path.join(MERGE_FIN_FILE_NAME);
        if merge_fin_file.is_file() {
            let merge_fin_file = DataFile::new_merge_fin_file(&self.options.dir_path)?;
            let merge_fin_record = merge_fin_file.read_log_record(0)?;
            let v = String::from_utf8(merge_fin_record.0.value).unwrap();

            non_merge_fid = v.parse::<u32>().unwrap();
            has_merge = true;
        }

        let mut transaction_records = HashMap::new();

        let active_file = self.active_file.read().unwrap();
        let old_files = self.old_files.read().unwrap();

        for (i, file_id) in self.file_ids.iter().enumerate() {
            // If the current has FILE_ID that less than NON_MERGE_FID, it indicates the current
            // file has already been loaded to the indexer via hint file, so we skip it.
            if has_merge && *file_id < non_merge_fid {
                continue;
            }

            // Read the file with id FILE_ID.
            let mut ofs = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(ofs),
                    false => {
                        let data_file = old_files.get(file_id).unwrap();
                        data_file.read_log_record(ofs)
                    }
                };

                let (mut log_record, size) = match log_record_res {
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

                // Load LOG_RECORD to memory.
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    ofs,
                };

                let (key, sequence_number) = parse_log_record_key(&log_record.key);
                if sequence_number == NON_TRANSACTION_SEQUENCE {
                    self.update_index(key, log_record.record_type, log_record_pos)?;
                } else {
                    if log_record.record_type == LogRecordType::TxnFinished {
                        let records: &Vec<TransactionRecord> =
                            transaction_records.get(&sequence_number).unwrap();
                        for txn_record in records.iter() {
                            self.update_index(
                                txn_record.record.key.clone(),
                                txn_record.record.record_type,
                                txn_record.pos,
                            )?;
                        }
                        transaction_records.remove(&sequence_number);
                    } else {
                        log_record.key = key;
                        transaction_records
                            .entry(sequence_number)
                            .or_insert(Vec::new())
                            .push(TransactionRecord {
                                record: log_record,
                                pos: log_record_pos,
                            });
                    }
                }

                if sequence_number > current_sequence_number {
                    current_sequence_number = sequence_number;
                }
                ofs += size as u64;
            }

            if i == self.file_ids.len() - 1 {
                active_file.set_write_ofs(ofs)
            }
        }

        Ok(current_sequence_number)
    }

    pub(crate) fn load_index_from_hint_file(&self) -> Result<()> {
        let hint_file_name = self.options.dir_path.join(HINT_FILE_NAME);

        // Return if hint file does not exist.
        if !hint_file_name.is_file() {
            return Ok(());
        }

        // Load all log records from hint file to the indexer.
        let hint_file = DataFile::new_hint_file(&hint_file_name)?;
        let mut ofs = 0;
        loop {
            let (log_record, size) = match hint_file.read_log_record(ofs) {
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
            let log_record_pos = decode_log_record_pos(log_record.value);
            self.index.put(log_record.key, log_record_pos);
            ofs += size as u64;
        }
        Ok(())
    }

    fn load_sequence_number(&self) -> (bool, usize) {
        let file_name = self.options.dir_path.join(SEQUENCE_NUMBER_FILE_NAME);
        if !file_name.is_file() {
            return (false, 0)
        }
        let sequence_number_file = DataFile::new_sequence_number_file(&self.options.dir_path).unwrap();
        let record = match sequence_number_file.read_log_record(0) {
            Ok(res) => res.0,
            Err(e) => panic!("failed to read sequence number: {:?}", e),
        };
        let v = String::from_utf8(record.value).unwrap();
        let sequence_number = v.parse::<usize>().unwrap();
        
        // Clean up after loading.
        fs::remove_file(file_name).unwrap();

        (true, sequence_number)
    }

    fn update_index(
        &self,
        key: Vec<u8>,
        record_type: LogRecordType,
        log_record_pos: LogRecordPos,
    ) -> Result<()> {
        match record_type {
            LogRecordType::Normal => self.index.put(key.clone(), log_record_pos),
            LogRecordType::Deleted => self.index.delete(key.clone()),
            _ => false,
        };
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            log::error!("error while closing engine: {:?}", e);
        }
    }
}

/// Fetch all data files under directory DIR_PATH.
fn load_data_files(dir_path: &PathBuf) -> Result<Vec<DataFile>> {
    let dir = fs::read_dir(dir_path);
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

/// Append the log record with the sequence number.
pub(crate) fn encode_log_record_key(key: Vec<u8>, sequence_number: usize) -> Vec<u8> {
    let mut encoded_key = BytesMut::new();
    encode_length_delimiter(sequence_number, &mut encoded_key).unwrap();
    encoded_key.extend_from_slice(&key.to_vec());
    encoded_key.to_vec()
}

/// Decode a encoded log record into the (key, sequence_number) pair.
pub(crate) fn parse_log_record_key(key: &Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf = BytesMut::new();
    buf.put_slice(key);
    let sequence_number = decode_length_delimiter(&mut buf).unwrap();
    (buf.to_vec(), sequence_number)
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
    use std::path::PathBuf;

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
        for i in 0..=100000 {
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
