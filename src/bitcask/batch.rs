//! A transaction is a group of database operations that is either
//! - successes, so the database state is updated, or
//! - failed, so the database rolls back to the original state priori to the transaction.
//!
//! A transaction needs to satisfy ACID principle, that is:
//! - Atomicity: Each transaction is treated as a single "unit".
//! - Consistency: A transaction can only bring the database from one consistent state to
//!     another, preserving database invariants: any data written to the database must be
//!     valid according to all defined rules.
//! - Isolation: concurrent execution of transactions leaves the database in the same
//!     state that would have been obtained if the transactions were executed sequentially.
//! - Durability: once a transaction has been committed, it will remain committed even
//!     in the case of a system failure.
//!
//! In my implementation, a global lock is used to provide guarantee transaction is at
//! isolation level of serializability, that is, concurrent transactions are performed as they
//! happened in serial.
//!
//! Tradeoff:
//! We can implement a MVCC (multi-version concurrency control) over bitcask, however, since the
//! log-structured storage model of bitcask, MVCC need to maintain all the records regarding their
//! key, indexing, and timestamps, this may insufficient as the disk memory grows rapidly.

use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc, Mutex},
};

use bytes::Bytes;

use super::{
    data::log_record::{LogRecord, LogRecordType}, 
    db::{encode_log_record_key, Engine}, 
    errors::{Errors, Result}, 
    options::{IndexType, WriteBatchOptions},
};

const TXN_FIN_KEY: &[u8] = "txn-fin".as_bytes();
pub(crate) const NON_TRANSACTION_SEQUENCE: usize = 0;

/// struct used for transaction write, where
/// - `pending_writes` is records all the incoming changes to the database.
/// - `engine` is a reference to the current bitcask instance, used to provide sequence
///     number to a transaction.
/// - `options` is the configuration for the transaction.
pub struct WriteBatch<'a> {
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    engine: &'a Engine,
    options: WriteBatchOptions,
}

impl Engine {
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        if self.options.index_type == IndexType::BPTree && !self.sequence_file_exists && !self.is_first_time_init {
            return Err(Errors::UnableToUseWriteBatch);
        }

        Ok(WriteBatch {
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options,
        })
    }
}

impl WriteBatch<'_> {
    /// Write the entry (KEY, VALUE) to the engine.
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::Normal,
        };

        let mut pending_write = self.pending_writes.lock().unwrap();
        pending_write.insert(key.to_vec(), log_record);

        Ok(())
    }

    /// Delete the entry with key KEY.
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let mut pending_write = self.pending_writes.lock().unwrap();
        let index_pos = self.engine.index.get(key.to_vec());
        if index_pos.is_none() {
            if pending_write.contains_key(&key.to_vec()) {
                pending_write.remove(&key.to_vec());
            }
            return Ok(());
        }

        let log_record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            record_type: LogRecordType::Deleted,
        };

        pending_write.insert(key.to_vec(), log_record);
        Ok(())
    }

    /// Commits all the changes to the engine, indicating the end of current transaction.
    pub fn commit(&self) -> Result<()> {
        let pending_writes = self.pending_writes.lock().unwrap();
        if pending_writes.len() == 0 {
            return Ok(());
        }
        if pending_writes.len() > self.options.max_batch_num {
            return Err(Errors::ExceedMaxBatchNum);
        }

        // Writes all the changes into the data file.
        let _batch_commit_lock = self.engine.batch_commit_lock.lock().unwrap();
        let sequence_number = self.engine.sequence_number.fetch_add(1, Ordering::SeqCst);
        let mut position = HashMap::new();
        for (_, item) in pending_writes.iter() {
            let mut log_record = LogRecord {
                key: encode_log_record_key(item.key.clone(), sequence_number),
                value: item.value.clone(),
                record_type: item.record_type,
            };
            let pos = self.engine.append_log_record(&mut log_record)?;
            position.insert(item.key.clone(), pos);
        }

        // Append a delimiter at the end of current commitment, which indicates the whole commit
        // is successful. On failure, we can roll back to the latest fin_record to ensure data
        // consistency.
        let mut fin_record = LogRecord {
            key: encode_log_record_key(TXN_FIN_KEY.to_vec(), sequence_number),
            value: Default::default(),
            record_type: LogRecordType::TxnFinished,
        };
        self.engine.append_log_record(&mut fin_record)?;

        if self.options.sync_writes {
            self.engine.sync()?;
        }

        // Update the indexer after commit.
        for (_, item) in pending_writes.iter() {
            match item.record_type {
                LogRecordType::Normal => {
                    let record_pos = position.get(&item.key).unwrap();
                    self.engine.index.put(item.key.clone(), *record_pos)
                }
                LogRecordType::Deleted => self.engine.index.delete(item.key.clone()),
                _ => false,
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::bitcask::{options::Options, utils};

    use super::*;

    #[test]
    fn test_write_batch_1() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-1");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create write batch");
        let put_res1 = wb.put(
            utils::rand_kv::get_test_key(1),
            utils::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            utils::rand_kv::get_test_key(2),
            utils::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());

        let res1 = engine.get(utils::rand_kv::get_test_key(1));
        assert_eq!(Errors::KeyNotFound, res1.err().unwrap());

        let commit_res = wb.commit();
        assert!(commit_res.is_ok());

        let res2 = engine.get(utils::rand_kv::get_test_key(1));
        assert!(res2.is_ok());

        let seq_no = wb.engine.sequence_number.load(Ordering::SeqCst);
        assert_eq!(2, seq_no);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_write_batch_2() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-2");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create write batch");
        let put_res1 = wb.put(
            utils::rand_kv::get_test_key(1),
            utils::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(
            utils::rand_kv::get_test_key(2),
            utils::rand_kv::get_test_value(10),
        );
        assert!(put_res2.is_ok());
        let commit_res1 = wb.commit();
        assert!(commit_res1.is_ok());

        let put_res3 = wb.put(
            utils::rand_kv::get_test_key(1),
            utils::rand_kv::get_test_value(10),
        );
        assert!(put_res3.is_ok());

        let commit_res2 = wb.commit();
        assert!(commit_res2.is_ok());

        // engine.close().expect("failed to close");
        // std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys();
        assert_eq!(2, keys.ok().unwrap().len());

        let seq_no = engine2.sequence_number.load(Ordering::SeqCst);
        assert_eq!(3, seq_no);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
