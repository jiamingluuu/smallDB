use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use crate::bitcask::data::log_record::LogRecordPos;
use crate::bitcask::index::Indexer;

pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut tree = self.tree.write().unwrap();
        tree.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let tree = self.tree.read().unwrap();
        tree.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut tree = self.tree.write().unwrap();
        let remove_res = tree.remove(&key);
        remove_res.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
            },
        );
        assert_eq!(res1, true);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                ofs: 22,
            },
        );
        assert_eq!(res2, true);
    }

    #[test]
    fn test_btree_get() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
            },
        );
        assert_eq!(res1, true);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                ofs: 22,
            },
        );
        assert_eq!(res1, true);

        let pos1 = bt.get("".as_bytes().to_vec());
        assert_eq!(pos1.unwrap().file_id, 1);
        assert_eq!(pos1.unwrap().ofs, 10);

        let pos1 = bt.get("aa".as_bytes().to_vec());
        assert_eq!(pos1.unwrap().file_id, 11);
        assert_eq!(pos1.unwrap().ofs, 22);
    }

    #[test]
    fn test_btree_delete() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
            },
        );
        assert_eq!(res1, true);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                ofs: 22,
            },
        );
        assert_eq!(res1, true);

        let del1 = bt.delete("".as_bytes().to_vec());
        assert!(del1);

        let del2 = bt.delete("aa".as_bytes().to_vec());
        assert!(del2);

        let del3 = bt.delete("a".as_bytes().to_vec());
        assert!(!del3);
    }
}
