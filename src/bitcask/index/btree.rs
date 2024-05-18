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
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
        let mut tree = self.tree.write().unwrap();
        tree.insert(key, pos)
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let tree = self.tree.read().unwrap();
        tree.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let mut tree = self.tree.write().unwrap();
        tree.remove(&key)
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

/// Iterator for BTree, where:
/// - `items` stores the key and log record position.
/// - `curr_index` indicates the position of iterator.
/// - `options` determines how to iterate through the BTree instance.
pub struct BTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>,
    curr_index: usize,
    options: IteratorOptions,
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
                size: 11,
            },
        );
        assert!(res1.is_none());

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                ofs: 22,
                size: 11,
            },
        );
        assert!(res2.is_none());

        let res3 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1144,
                ofs: 22122,
                size: 11,
            },
        );
        assert!(res3.is_some());
        let v = res3.unwrap();
        assert_eq!(v.file_id, 11);
        assert_eq!(v.ofs, 22);
    }

    #[test]
    fn test_btree_get() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                ofs: 22,
                size: 11,
            },
        );
        assert!(res2.is_none());

        let pos1 = bt.get("".as_bytes().to_vec());
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id, 1);
        assert_eq!(pos1.unwrap().ofs, 10);

        let pos2 = bt.get("aa".as_bytes().to_vec());
        assert!(pos2.is_some());
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
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                ofs: 22,
                size: 11,
            },
        );
        assert!(res2.is_none());

        let del1 = bt.delete("".as_bytes().to_vec());
        assert!(del1.is_some());
        let v1 = del1.unwrap();
        assert_eq!(v1.file_id, 1);
        assert_eq!(v1.ofs, 10);

        let del2 = bt.delete("aa".as_bytes().to_vec());
        assert!(del2.is_some());
        let v2 = del2.unwrap();
        assert_eq!(v2.file_id, 11);
        assert_eq!(v2.ofs, 22);

        let del3 = bt.delete("not exist".as_bytes().to_vec());
        assert!(del3.is_none());
    }

    #[test]
    fn test_btree_iterator_seek() {
        let bt = BTree::new();

        let mut iter1 = bt.iterator(IteratorOptions::default());
        iter1.seek("aa".as_bytes().to_vec());
        let res1 = iter1.next();
        assert!(res1.is_none());

        bt.put(
            "ccde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
                size: 11,
            },
        );
        let mut iter2 = bt.iterator(IteratorOptions::default());
        iter2.seek("aa".as_bytes().to_vec());
        let res2 = iter2.next();
        assert!(res2.is_some());

        let mut iter3 = bt.iterator(IteratorOptions::default());
        iter3.seek("zz".as_bytes().to_vec());
        let res3 = iter3.next();
        assert!(res3.is_none());

        bt.put(
            "bbed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
                size: 11,
            },
        );
        bt.put(
            "aaed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
                size: 11,
            },
        );
        bt.put(
            "cadd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
                size: 11,
            },
        );

        let mut iter4 = bt.iterator(IteratorOptions::default());
        iter4.seek("b".as_bytes().to_vec());
        while let Some(item) = iter4.next() {
            assert!(item.0.len() > 0);
        }

        let mut iter5 = bt.iterator(IteratorOptions::default());
        iter5.seek("cadd".as_bytes().to_vec());
        while let Some(item) = iter5.next() {
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        let mut iter6 = bt.iterator(IteratorOptions::default());
        iter6.seek("zzz".as_bytes().to_vec());
        let res6 = iter6.next();
        assert!(res6.is_none());

        let mut iter_opts = IteratorOptions::default();
        iter_opts.reverse = true;
        let mut iter7 = bt.iterator(iter_opts);
        iter7.seek("bb".as_bytes().to_vec());
        while let Some(item) = iter7.next() {
            assert!(item.0.len() > 0);
        }
    }

    #[test]
    fn test_btree_iterator_next() {
        let bt = BTree::new();
        let mut iter1 = bt.iterator(IteratorOptions::default());
        assert!(iter1.next().is_none());

        bt.put(
            "cadd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
                size: 11,
            },
        );
        let mut iter_opt1 = IteratorOptions::default();
        iter_opt1.reverse = true;
        let mut iter2 = bt.iterator(iter_opt1);
        assert!(iter2.next().is_some());

        bt.put(
            "bbed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
                size: 11,
            },
        );
        bt.put(
            "aaed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
                size: 11,
            },
        );
        bt.put(
            "cdea".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                ofs: 10,
                size: 11,
            },
        );

        let mut iter_opt2 = IteratorOptions::default();
        iter_opt2.reverse = true;
        let mut iter3 = bt.iterator(iter_opt2);
        while let Some(item) = iter3.next() {
            assert!(item.0.len() > 0);
        }

        let mut iter_opt3 = IteratorOptions::default();
        iter_opt3.prefix = "bbed".as_bytes().to_vec();
        let mut iter4 = bt.iterator(iter_opt3);
        while let Some(item) = iter4.next() {
            assert!(item.0.len() > 0);
        }
    }
}
