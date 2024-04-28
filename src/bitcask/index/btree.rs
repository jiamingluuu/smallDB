use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use bytes::Bytes;

use crate::bitcask::{
    data::log_record::LogRecordPos,
    errors::Result,
    index::{IndexIterator, Indexer},
    options::IteratorOptions,
};

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

    fn list_keys(&self) -> Result<Vec<Bytes>> {
        let read_guard = self.tree.read().unwrap();
        let mut keys = Vec::with_capacity(read_guard.len());
        for (k, _) in read_guard.iter() {
            keys.push(Bytes::copy_from_slice(&k));
        }
        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read().unwrap();
        let mut items = Vec::with_capacity(read_guard.len());
        // 将 BTree 中的数据存储到数组中
        for (key, value) in read_guard.iter() {
            items.push((key.clone(), value.clone()));
        }
        if options.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            curr_index: 0,
            options,
        })
    }
}

/// Iterator for BTree
pub struct BTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>, // Storing the key and log record position
    curr_index: usize,        // the current position of iterator.
    options: IteratorOptions, // the config for iterator.
}

impl IndexIterator for BTreeIterator {
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_val) => equal_val,
            Err(insert_val) => insert_val,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }

        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
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
        assert_eq!(res2, true);

        let pos1 = bt.get("".as_bytes().to_vec());
        assert_eq!(pos1.unwrap().file_id, 1);
        assert_eq!(pos1.unwrap().ofs, 10);

        let pos2 = bt.get("aa".as_bytes().to_vec());
        assert_eq!(pos2.unwrap().file_id, 11);
        assert_eq!(pos2.unwrap().ofs, 22);
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
        assert_eq!(res2, true);

        let del1 = bt.delete("".as_bytes().to_vec());
        assert!(del1);

        let del2 = bt.delete("aa".as_bytes().to_vec());
        assert!(del2);

        let del3 = bt.delete("a".as_bytes().to_vec());
        assert!(!del3);
    }
}
