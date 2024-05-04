//! The main focus of merge is to clean redundancy on disk caused by using bitcask.
//! On merging the data file of bitcask instance A, we do the followings:
//! 1. Create a tmp directory and a new bitcask instance B.
//! 2. Fetch all the log records from A's data file directory and add the record into the B's
//!     merge directory by checking LogRecordType with the indexer.
//! 3. After merge completes, create a hint file next to each data files, which is just a
//!     data file but instead of storing the value, it contains the position and size of the
//!     values within the corresponding data file.

use std::{fs, path::PathBuf};

use super::{
    batch::NON_TRANSACTION_SEQUENCE,
    data::{
        data_file::{get_data_file_name, DataFile, MERGE_FIN_FILE_NAME, SEQUENCE_NUMBER_FILE_NAME},
        log_record::{LogRecord, LogRecordType},
    },
    db::{encode_log_record_key, parse_log_record_key, Engine, LOCK_FILE_NAME},
    errors::{Errors, Result},
    options::{IOType, Options},
};

const MERGE_DIR_NAME: &str = "merge";
const MERGE_FIN_KEY: &[u8] = "merge-finished".as_bytes();

impl Engine {
    /// Atomically merge the data file under the current bitcask working directory. During the
    /// merge process, we clean all the deleted log record and construct a hint file used to
    /// speed up the engine startup time.
    pub fn merge(&self) -> Result<()> {
        let _merge_lock = self
            .merge_lock
            .try_lock()
            .map_err(|_| Errors::MergeInProgress)?;

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

    /// Get the list of all data files. Close and replace the current active file with a new one.
    fn get_merge_files(&self) -> Result<Vec<DataFile>> {
        // Get all the file id of all old files.
        let mut old_files = self.old_files.write().unwrap();
        let mut merge_file_ids: Vec<u32> = old_files.iter().map(|(k, _)| *k).collect();

        // Get the file id of active file, and close the current active file.
        let mut active_file = self.active_file.write().unwrap();
        active_file.sync()?;
        let active_file_id = active_file.get_file_id();
        let new_active_file = DataFile::new(&self.options.dir_path, active_file_id + 1, IOType::StandaradFIO)?;
        *active_file = new_active_file;
        let old_file = DataFile::new(&self.options.dir_path, active_file_id, IOType::StandaradFIO)?;
        old_files.insert(active_file_id, old_file);

        merge_file_ids.push(active_file_id);
        merge_file_ids.sort();

        let mut merge_files = Vec::new();
        for fid in &merge_file_ids {
            let data_file = DataFile::new(&self.options.dir_path, *fid, IOType::StandaradFIO)?;
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
