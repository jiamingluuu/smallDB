use std::sync::Arc;

use bytes::Bytes;
use std::sync::RwLock;

use super::{
    db::Engine, 
    errors::Result, 
    index::IndexIterator, 
    options::IteratorOptions,
};

pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>,
    engine: &'a Engine,
}

impl Engine {
    /// Get the iterator instance.
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }

    /// Get all the keys contained in the engine.
    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }

    /// Invoke function F for all (key, value) pairs contained in the engine.
    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        Self: Sized,
        F: Fn(Bytes, Bytes) -> bool,
    {
        let iter = self.iter(IteratorOptions::default());
        while let Some((key, value)) = iter.next() {
            if !f(key, value) {
                break;
            }
        }
        Ok(())
    }
}

impl Iterator<'_> {
    pub fn rewind(&self) {
        let mut index_iter = self.index_iter.write().unwrap();
        index_iter.rewind();
    }

    pub fn seek(&self, key: Vec<u8>) {
        let mut index_iter = self.index_iter.write().unwrap();
        index_iter.seek(key);
    }

    pub fn next(&self) -> Option<(Bytes, Bytes)> {
        let mut index_iter = self.index_iter.write().unwrap();
        if let Some(item) = index_iter.next() {
            let value = self
                .engine
                .get_value_by_position(item.1)
                .expect("failed to get value from data file");
            return Some((Bytes::from(item.0.to_vec()), value));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::bitcask::{options::Options, utils};

    use super::*;

    #[test]
    fn test_list_keys() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-list-keys");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let keys1 = engine.list_keys();
        assert_eq!(keys1.ok().unwrap().len(), 0);

        let put_res1 = engine.put(Bytes::from("aacc"), utils::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(Bytes::from("eecc"), utils::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), utils::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), utils::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        let keys2 = engine.list_keys();
        assert_eq!(keys2.ok().unwrap().len(), 4);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_fold() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-fold");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res1 = engine.put(Bytes::from("aacc"), utils::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(Bytes::from("eecc"), utils::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), utils::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), utils::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        engine
            .fold(|key, value| {
                assert!(key.len() > 0);
                assert!(value.len() > 0);
                return true;
            })
            .unwrap();

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_iterator_seek() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iter-seek");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let iter1 = engine.iter(IteratorOptions::default());
        iter1.seek("aa".as_bytes().to_vec());
        assert!(iter1.next().is_none());

        let put_res1 = engine.put(Bytes::from("aacc"), utils::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let iter2 = engine.iter(IteratorOptions::default());
        iter2.seek("a".as_bytes().to_vec());
        assert!(iter2.next().is_some());

        let put_res2 = engine.put(Bytes::from("eecc"), utils::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), utils::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), utils::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        let iter3 = engine.iter(IteratorOptions::default());
        iter3.seek("a".as_bytes().to_vec());
        assert_eq!(Bytes::from("aacc"), iter3.next().unwrap().0);

        let put_res2 = engine.put(Bytes::from("aade"), utils::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("ddce"), utils::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("bbcc"), utils::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        let mut iter_opts1 = IteratorOptions::default();
        iter_opts1.reverse = true;
        let iter2 = engine.iter(iter_opts1);
        while let Some(item) = iter2.next() {
            assert!(item.0.len() > 0);
        }

        // delete the testing file.
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_iterator_prefix() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iter-prefix");
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res1 = engine.put(Bytes::from("eecc"), utils::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(Bytes::from("aade"), utils::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("ddce"), utils::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("bbcc"), utils::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());
        let put_res4 = engine.put(Bytes::from("ddaa"), utils::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        let mut iter_opt1 = IteratorOptions::default();
        iter_opt1.prefix = "dd".as_bytes().to_vec();
        let iter1 = engine.iter(iter_opt1);
        while let Some(item) = iter1.next() {
            assert!(item.0.len() > 0);
        }

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
