//! The main focus of merge is to clean redundancy on disk caused by using bitcask.
//! On merging the data file of bitcask instance A, we do the followings:
//! 1. Create a tmp directory and a new bitcask instance B.
//! 2. Fetch all the log records from A's data file directory and add the record into the B's
//!     merge directory by checking LogRecordType with the indexer.
//! 3. After merge completes, create a hint file next to each data files, which is just a
//!     data file but instead of storing the value, it contains the position and size of the
//!     values within the corresponding data file.

use std::{fs, path::PathBuf, sync::atomic::Ordering};

use crate::{
    batch::NON_TRANSACTION_SEQUENCE,
    data::{
        data_file::{
            get_data_file_name, DataFile, DATA_FILE_NAME_SUFFIX, MERGE_FIN_FILE_NAME,
            SEQUENCE_NUMBER_FILE_NAME,
        },
        log_record::{LogRecord, LogRecordType},
    },
    db::{encode_log_record_key, parse_log_record_key, Engine, LOCK_FILE_NAME},
    errors::{Errors, Result},
    options::{IOType, Options},
    utils,
};

const MERGE_DIR_NAME: &str = "merge";
const MERGE_FIN_KEY: &[u8] = "merge-finished".as_bytes();

impl Engine {
    /// Atomically merge the data file under the current bitcask working directory. During the
    /// merge process, we clean all the deleted log record and construct a hint file used to
    /// speed up the engine startup time.
    pub fn merge(&self) -> Result<()> {
        if self.is_empty_engine() {
            return Ok(());
        }

        let _merge_lock = self
            .merge_lock
            .try_lock()
            .map_err(|_| Errors::MergeInProgress)?;

        let reclaim_size = self.reclaim_size.load(Ordering::SeqCst);
        let total_size = utils::file::dir_disk_size(&self.options.dir_path);
        if (reclaim_size as f32) / (total_size as f32) < self.options.data_file_merge_ratio {
            return Err(Errors::MergeRationUnreached);
        }

        let available_size = utils::file::available_disk_size();
        if total_size - reclaim_size as u64 > available_size {
            return Err(Errors::MergeNoEnoughSpace);
        }

        let merge_path = get_merge_path(&self.options.dir_path);
        if merge_path.is_dir() {
            fs::remove_dir_all(merge_path.clone()).unwrap();
        }
        fs::create_dir_all(merge_path.clone()).map_err(|_| Errors::FailedToCreateDatabaseDir)?;

        // Obtain all the live files
        let merge_files = self.get_merge_files()?;
        let mut merge_engine_opts = Options::default();
        merge_engine_opts.dir_path = merge_path.clone();
        merge_engine_opts.data_file_size = self.options.data_file_size;
        let merge_engine = Engine::open(merge_engine_opts)?;

        // Create the hint file.
        let hint_file = DataFile::new_hint_file(&merge_path)?;
        for data_file in &merge_files {
            let mut ofs = 0;
            loop {
                let (mut log_record, size) = match data_file.read_log_record(ofs) {
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

                // Write live log records to the data file,
                // create a hint file next to each data file.
                let (key, _) = parse_log_record_key(&log_record.key);
                if let Some(index_pos) = self.index.get(key.clone()) {
                    if index_pos.file_id == data_file.get_file_id() && index_pos.ofs == ofs {
                        log_record.key =
                            encode_log_record_key(key.clone(), NON_TRANSACTION_SEQUENCE);
                        let log_record_pos = merge_engine.append_log_record(&mut log_record)?;
                        hint_file.write_hint_record(key.clone(), log_record_pos)?;
                    }
                }

                ofs += size as u64;
            }
        }

        // Synchronize all the metadata to the disk
        merge_engine.sync()?;
        hint_file.sync()?;

        // Append the data file with a fin_record indicating merge process is completed.
        let non_merge_file_id = merge_files.last().unwrap().get_file_id() + 1;
        let merge_fin_file = DataFile::new_merge_fin_file(&merge_path)?;
        let merge_fin_record = LogRecord {
            key: MERGE_FIN_KEY.to_vec(),
            value: non_merge_file_id.to_string().into_bytes(),
            record_type: LogRecordType::Normal,
        };

        let encoded_record = merge_fin_record.encode();
        merge_fin_file.write(&encoded_record)?;
        merge_fin_file.sync()?;

        Ok(())
    }

    fn is_empty_engine(&self) -> bool {
        let active_file = self.active_file.read().unwrap();
        let old_files = self.old_files.read().unwrap();
        if active_file.get_write_ofs() == 0 && old_files.len() == 0 {
            return true;
        }
        false
    }

    /// Get the list of all data files. Close and replace the current active file with a new one.
    fn get_merge_files(&self) -> Result<Vec<DataFile>> {
        // Get all the file id of all old files.
        let mut old_files = self.old_files.write().unwrap();
        let mut merge_file_ids: Vec<u32> = old_files.iter().map(|(k, _)| *k).collect();

        // Get the file id of active file, and close the current active file.
        let mut active_file = self.active_file.write().unwrap();
        active_file.sync()?;
        let active_file_id = active_file.get_file_id();
        let new_active_file = DataFile::new(
            &self.options.dir_path,
            active_file_id + 1,
            IOType::StandardFIO,
        )?;
        *active_file = new_active_file;
        let old_file = DataFile::new(&self.options.dir_path, active_file_id, IOType::StandardFIO)?;
        old_files.insert(active_file_id, old_file);

        merge_file_ids.push(active_file_id);
        merge_file_ids.sort();

        let mut merge_files = Vec::new();
        for fid in &merge_file_ids {
            let data_file = DataFile::new(&self.options.dir_path, *fid, IOType::StandardFIO)?;
            merge_files.push(data_file);
        }

        Ok(merge_files)
    }
}

/// Append DIR_PATH with "merge" suffix, which is the default directory name used for merge process.
fn get_merge_path(dir_path: &PathBuf) -> PathBuf {
    let file_name = dir_path.file_name().unwrap();
    let merge_path = std::format!("{}-{}", file_name.to_str().unwrap(), MERGE_DIR_NAME);
    let parent = dir_path.parent().unwrap();
    parent.to_path_buf().join(merge_path)
}

/// Load all data file from the merge directory to DIR_PATH.
pub(crate) fn load_merge_files(dir_path: &PathBuf) -> Result<()> {
    let merge_path = get_merge_path(dir_path);

    // If the directory does not exists, it indicates no merge happened, return.
    if !merge_path.is_dir() {
        return Ok(());
    }

    // Check if the merge-fin file exists.
    let mut merge_file_names = Vec::new();
    let mut merge_finished = false;
    let dir = fs::read_dir(merge_path.clone()).map_err(|_| Errors::FailedToReadDatabaseDir)?;
    for file in dir {
        if let Ok(entry) = file {
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();
            if file_name.ends_with(MERGE_FIN_FILE_NAME) {
                merge_finished = true;
            }

            // Ignore the file indicates the sequence number. It is possible to have a new
            // transaction happens during the merge process, so the old sequence number file
            // is outdated.
            if file_name.ends_with(SEQUENCE_NUMBER_FILE_NAME) || file_name.ends_with(LOCK_FILE_NAME)
            {
                continue;
            }

            // Skip empty files.
            let meta = entry.metadata().unwrap();
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) && meta.len() == 0 {
                continue;
            }

            merge_file_names.push(entry.file_name());
        }
    }

    // Merge-fin file does not exist indicates merge process is not completed due to a undesired
    // behavior, for instance, system shutdown. So we deletes the whole merge directory to
    // discard the merge process.
    if !merge_finished {
        fs::remove_dir_all(merge_path.clone()).unwrap();
        return Ok(());
    }

    // Delete all non-merged file.
    let merge_fin_file = DataFile::new_merge_fin_file(&merge_path)?;
    let merge_fin_record = merge_fin_file.read_log_record(0)?;
    let v = String::from_utf8(merge_fin_record.0.value).unwrap();
    let non_merge_fid = v.parse::<u32>().unwrap();
    for file_id in 0..non_merge_fid {
        let file = get_data_file_name(dir_path, file_id);
        if file.is_file() {
            fs::remove_file(file).unwrap();
        }
    }

    // Move merged data file to the current bitcask working directory.
    for file_name in merge_file_names {
        let from = merge_path.join(file_name.clone());
        let to = dir_path.join(file_name.clone());
        fs::rename(from, to).unwrap();
    }

    fs::remove_dir_all(merge_path.clone()).unwrap();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::rand_kv::{get_test_key, get_test_value};
    use bytes::Bytes;
    use std::{sync::Arc, thread};

    #[test]
    fn test_merge_1() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-1");
        opts.data_file_size = 32 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let res1 = engine.merge();
        assert!(res1.is_ok());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_merge_2() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-2");
        opts.data_file_size = 32 * 1024 * 1024;
        opts.data_file_merge_ratio = 0 as f32;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        for i in 0..50000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }

        let res1 = engine.merge();
        assert!(res1.is_ok());

        std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys().unwrap();
        assert_eq!(keys.len(), 50000);

        for i in 0..50000 {
            let get_res = engine2.get(get_test_key(i));
            assert!(get_res.ok().unwrap().len() > 0);
        }

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_merge_3() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-3");
        opts.data_file_size = 32 * 1024 * 1024;
        opts.data_file_merge_ratio = 0 as f32;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        for i in 0..50000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }
        for i in 0..10000 {
            let put_res = engine.put(get_test_key(i), Bytes::from("new value in merge"));
            assert!(put_res.is_ok());
        }
        for i in 40000..50000 {
            let del_res = engine.delete(get_test_key(i));
            assert!(del_res.is_ok());
        }

        let res1 = engine.merge();
        assert!(res1.is_ok());

        std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys().unwrap();
        assert_eq!(keys.len(), 40000);

        for i in 0..10000 {
            let get_res = engine2.get(get_test_key(i));
            assert_eq!(Bytes::from("new value in merge"), get_res.ok().unwrap());
        }

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_merge_4() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-4");
        opts.data_file_size = 32 * 1024 * 1024;
        opts.data_file_merge_ratio = 0 as f32;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        for i in 0..50000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
            let del_res = engine.delete(get_test_key(i));
            assert!(del_res.is_ok());
        }

        let res1 = engine.merge();
        assert!(res1.is_ok());

        std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys().unwrap();
        assert_eq!(keys.len(), 0);

        for i in 0..50000 {
            let get_res = engine2.get(get_test_key(i));
            assert_eq!(Errors::KeyNotFound, get_res.err().unwrap());
        }

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_merge_5() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-5");
        opts.data_file_size = 32 * 1024 * 1024;
        opts.data_file_merge_ratio = 0 as f32;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        for i in 0..50000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }
        for i in 0..10000 {
            let put_res = engine.put(get_test_key(i), Bytes::from("new value in merge"));
            assert!(put_res.is_ok());
        }
        for i in 40000..50000 {
            let del_res = engine.delete(get_test_key(i));
            assert!(del_res.is_ok());
        }

        let eng = Arc::new(engine);

        let mut handles = vec![];
        let eng1 = eng.clone();
        let handle1 = thread::spawn(move || {
            for i in 60000..100000 {
                let put_res = eng1.put(get_test_key(i), get_test_value(i));
                assert!(put_res.is_ok());
            }
        });
        handles.push(handle1);

        let eng2 = eng.clone();
        let handle2 = thread::spawn(move || {
            let merge_res = eng2.merge();
            assert!(merge_res.is_ok());
        });
        handles.push(handle2);

        for handle in handles {
            handle.join().unwrap();
        }

        std::mem::drop(eng);
        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys().unwrap();
        assert_eq!(keys.len(), 80000);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
