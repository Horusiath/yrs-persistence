//! **yrs-rocksdb** is a persistence layer allowing to store [Yrs](https://docs.rs/yrs/latest/yrs/index.html)
//! documents and providing convenient utility functions to work with them, using RocksDB for persistent backed.
//!
//! # Example
//!
//! ```rust
//! use std::sync::Arc;
//! use rocksdb::TransactionDB;
//! use yrs::{Doc, Text, Transact};
//! use yrs_kvstore::DocOps;
//! use yrs_rocksdb::RocksDBStore;
//!
//! let db: Arc<TransactionDB> = Arc::new(TransactionDB::open_default("my-db-path").unwrap());
//!
//! let doc = Doc::new();
//! let text = doc.get_or_insert_text("text");
//!
//! // restore document state from DB
//! {
//!   let db_txn = RocksDBStore::from(db.transaction());
//!   db_txn.load_doc("my-doc-name", &mut doc.transact_mut()).unwrap();
//! }
//!
//! // another options is to flush document state right away, but
//! // this requires a read-write transaction
//! {
//!   let db_txn = RocksDBStore::from(db.transaction());
//!   let doc = db_txn.flush_doc_with("my-doc-name", yrs::Options::default()).unwrap();
//!   db_txn.commit().unwrap(); // flush may change store state
//! }
//!
//! // configure document to persist every update and
//! // occassionaly compact them into document state
//! let sub = {
//!   let db = db.clone();
//!   let options = doc.options().clone();
//!   doc.observe_update_v1(move |_,e| {
//!       let db_txn = RocksDBStore::from(db.transaction());
//!       let seq_nr = db_txn.push_update("my-doc-name", &e.update).unwrap();
//!       if seq_nr % 64 == 0 {
//!           // occassinally merge updates into the document state
//!           db_txn.flush_doc_with("my-doc-name", options.clone()).unwrap();
//!       }
//!       db_txn.commit().unwrap();
//!   })
//! };
//!
//! text.insert(&mut doc.transact_mut(), 0, "a");
//! text.insert(&mut doc.transact_mut(), 1, "b");
//! text.insert(&mut doc.transact_mut(), 2, "c");
//! ```

use rocksdb::{
    DBIteratorWithThreadMode, DBPinnableSlice, Direction, IteratorMode, ReadOptions, Transaction,
};
use std::ops::Deref;
use yrs_kvstore::{DocOps, KVEntry, KVStore};

pub use yrs_kvstore as store;

/// Type wrapper around RocksDB [Transaction] struct. Used to extend it with [DocOps]
/// methods used for convenience when working with Yrs documents.
#[repr(transparent)]
pub struct RocksDBStore<'a, DB>(Transaction<'a, DB>);

impl<'a, DB> RocksDBStore<'a, DB> {
    #[inline(always)]
    pub fn commit(self) -> Result<(), rocksdb::Error> {
        self.0.commit()
    }
}

impl<'a, DB> From<Transaction<'a, DB>> for RocksDBStore<'a, DB> {
    #[inline(always)]
    fn from(txn: Transaction<'a, DB>) -> Self {
        RocksDBStore(txn)
    }
}

impl<'a, DB> Into<Transaction<'a, DB>> for RocksDBStore<'a, DB> {
    #[inline(always)]
    fn into(self) -> Transaction<'a, DB> {
        self.0
    }
}

impl<'a, DB> Deref for RocksDBStore<'a, DB> {
    type Target = Transaction<'a, DB>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, DB> DocOps<'a> for RocksDBStore<'a, DB> {}

impl<'a, DB> KVStore<'a> for RocksDBStore<'a, DB> {
    type Error = rocksdb::Error;
    type Cursor = RocksDBIter<'a, DB>;
    type Entry = RocksDBEntry;
    type Return = DBPinnableSlice<'a>;

    fn get(&self, key: &[u8]) -> Result<Option<Self::Return>, Self::Error> {
        if let Some(pinned) = self.0.get_pinned(key)? {
            Ok(Some(unsafe { std::mem::transmute(pinned) }))
        } else {
            Ok(None)
        }
    }

    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.0.put(key, value)?;
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.0.delete(key)?;
        Ok(())
    }

    fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error> {
        let mut opt = ReadOptions::default();
        opt.set_iterate_lower_bound(from);
        opt.set_iterate_upper_bound(to);
        let mut i = self
            .0
            .iterator_opt(IteratorMode::From(from, Direction::Forward), opt);
        while let Some(res) = i.next() {
            let (key, _) = res?;
            self.0.delete(key)?;
        }
        Ok(())
    }

    fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error> {
        let mut opt = ReadOptions::default();
        opt.set_iterate_lower_bound(from);
        opt.set_iterate_upper_bound(to);
        let raw = self
            .0
            .iterator_opt(IteratorMode::From(from, Direction::Forward), opt);
        Ok(RocksDBIter::new(
            unsafe { std::mem::transmute(raw) },
            to.to_vec(),
        ))
    }

    fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error> {
        let opt = ReadOptions::default();
        let mut raw = self.0.raw_iterator_opt(opt);
        raw.seek_for_prev(key);
        if let Some((key, value)) = raw.item() {
            Ok(Some(RocksDBEntry::new(key.into(), value.into())))
        } else {
            Ok(None)
        }
    }
}

pub struct RocksDBIter<'a, DB> {
    inner: DBIteratorWithThreadMode<'a, Transaction<'a, DB>>,
    to: Vec<u8>,
}

impl<'a, DB> RocksDBIter<'a, DB> {
    fn new(inner: DBIteratorWithThreadMode<'a, Transaction<'a, DB>>, to: Vec<u8>) -> Self {
        RocksDBIter { inner, to }
    }
}

impl<'a, DB> Iterator for RocksDBIter<'a, DB> {
    type Item = RocksDBEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let n = self.inner.next()?;
        if let Ok((key, value)) = n {
            if key.as_ref() >= &self.to {
                None
            } else {
                Some(RocksDBEntry::new(key, value))
            }
        } else {
            None
        }
    }
}

pub struct RocksDBEntry {
    key: Box<[u8]>,
    value: Box<[u8]>,
}

impl RocksDBEntry {
    fn new(key: Box<[u8]>, value: Box<[u8]>) -> Self {
        RocksDBEntry { key, value }
    }
}

impl Into<(Box<[u8]>, Box<[u8]>)> for RocksDBEntry {
    fn into(self) -> (Box<[u8]>, Box<[u8]>) {
        (self.key, self.value)
    }
}

impl KVEntry for RocksDBEntry {
    fn key(&self) -> &[u8] {
        &self.key
    }

    fn value(&self) -> &[u8] {
        &self.value
    }
}

#[cfg(test)]
mod test {
    use crate::RocksDBStore;
    use rocksdb::TransactionDB;
    use std::path::Path;
    use std::sync::Arc;
    use tempdir::TempDir;
    use crate::store::yrs::{Doc, GetString, ReadTxn, Text, Transact};
    use crate::store::DocOps;

    fn init_env<P: AsRef<Path>>(dir: P) -> TransactionDB {
        let db = TransactionDB::open_default(dir).unwrap();
        db
    }

    #[test]
    fn create_get_remove() {
        let tmp = TempDir::new("rocksdb-create_get_remove").unwrap();
        let db = init_env(&tmp);

        // insert document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, "hello");

            let db_txn = RocksDBStore::from(db.transaction());
            db_txn.insert_doc("doc", &txn).unwrap();
            db_txn.commit().unwrap();
        }

        // retrieve document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            let db_txn = RocksDBStore::from(db.transaction());
            db_txn.load_doc("doc", &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "hello");

            let (sv, completed) = db_txn.get_state_vector("doc").unwrap();
            assert_eq!(sv, Some(txn.state_vector()));
            assert!(completed);
        }

        // remove document
        {
            let db_txn = RocksDBStore::from(db.transaction());

            db_txn.clear_doc("doc").unwrap();

            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            db_txn.load_doc("doc", &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "");

            let (sv, completed) = db_txn.get_state_vector("doc").unwrap();
            assert!(sv.is_none());
            assert!(completed);
        }
    }
    #[test]
    fn multi_insert() {
        let tmp = TempDir::new("rocksdb-multi_insert").unwrap();
        let db = init_env(&tmp);

        // insert document twice
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello");

            let db_txn = RocksDBStore::from(db.transaction());

            db_txn.insert_doc("doc", &txn).unwrap();

            text.push(&mut txn, " world");

            db_txn.insert_doc("doc", &txn).unwrap();
            db_txn.commit().unwrap();
        }

        // retrieve document
        {
            let db_txn = RocksDBStore::from(db.transaction());

            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            db_txn.load_doc("doc", &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "hello world");
        }
    }

    #[test]
    fn incremental_updates() {
        const DOC_NAME: &str = "doc";
        let tmp = TempDir::new("rocksdb-incremental_updates").unwrap();
        let db = init_env(&tmp);
        let db = Arc::new(db);

        // store document updates
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");

            let db = db.clone();
            let _sub = doc.observe_update_v1(move |_, u| {
                let db_txn = RocksDBStore::from(db.transaction());
                db_txn.push_update(DOC_NAME, &u.update).unwrap();
                db_txn.commit().unwrap();
            });
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            text.push(&mut doc.transact_mut(), "c");
        }

        // load document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();

            let db_txn = RocksDBStore::from(db.transaction());
            db_txn.load_doc(DOC_NAME, &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "abc");
        }

        // flush document
        {
            let db_txn = RocksDBStore::from(db.transaction());
            let doc = db_txn.flush_doc(DOC_NAME).unwrap().unwrap();
            db_txn.commit().unwrap();

            let text = doc.get_or_insert_text("text");

            assert_eq!(text.get_string(&doc.transact()), "abc");
        }
    }

    #[test]
    fn state_vector_updates_only() {
        const DOC_NAME: &str = "doc";
        let tmp = TempDir::new("rocksdb-state_vector_updates_only").unwrap();
        let db = init_env(&tmp);
        let db = Arc::new(db);

        // store document updates
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let db = db.clone();
            let _sub = doc.observe_update_v1(move |_, u| {
                let db_txn = RocksDBStore::from(db.transaction());
                db_txn.push_update(DOC_NAME, &u.update).unwrap();
                db_txn.commit().unwrap();
            });
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            text.push(&mut doc.transact_mut(), "c");

            let sv = doc.transact().state_vector();
            sv
        };

        let db_txn = RocksDBStore::from(db.transaction());
        let (sv, completed) = db_txn.get_state_vector(DOC_NAME).unwrap();
        assert!(sv.is_none());
        assert!(!completed); // since it's not completed, we should recalculate state vector from doc state
    }

    #[test]
    fn state_diff_from_updates() {
        const DOC_NAME: &str = "doc";
        let tmp = TempDir::new("rocksdb-state_diff_from_updates").unwrap();
        let db = init_env(&tmp);
        let db = Arc::new(db);

        let (sv, expected) = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");

            let db = db.clone();
            let _sub = doc.observe_update_v1(move |_, u| {
                let db_txn = RocksDBStore::from(db.transaction());
                db_txn.push_update(DOC_NAME, &u.update).unwrap();
                db_txn.commit().unwrap();
            });

            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            let sv = doc.transact().state_vector();
            text.push(&mut doc.transact_mut(), "c");
            let update = doc.transact().encode_diff_v1(&sv);
            (sv, update)
        };

        let db_txn = RocksDBStore::from(db.transaction());
        let actual = db_txn.get_diff(DOC_NAME, &sv).unwrap();
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn state_diff_from_doc() {
        const DOC_NAME: &str = "doc";
        let tmp = TempDir::new("rocksdb-state_diff_from_doc").unwrap();
        let db = init_env(&tmp);
        let db = Arc::new(db);

        let (sv, expected) = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            let sv = doc.transact().state_vector();
            text.push(&mut doc.transact_mut(), "c");
            let update = doc.transact().encode_diff_v1(&sv);

            let db_txn = RocksDBStore::from(db.transaction());
            db_txn.insert_doc(DOC_NAME, &doc.transact()).unwrap();
            db_txn.commit().unwrap();

            (sv, update)
        };

        let db_txn = RocksDBStore::from(db.transaction());
        let actual = db_txn.get_diff(DOC_NAME, &sv).unwrap();
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn doc_meta() {
        const DOC_NAME: &str = "doc";
        let tmp = TempDir::new("rocksdb-doc_meta").unwrap();
        let db = init_env(&tmp);
        let db = Arc::new(db);

        let db_txn = RocksDBStore::from(db.transaction());
        let value = db_txn.get_meta(DOC_NAME, "key").unwrap();
        assert!(value.is_none());
        db_txn
            .insert_meta(DOC_NAME, "key", "value1".as_bytes())
            .unwrap();
        db_txn.commit().unwrap();

        let db_txn = RocksDBStore::from(db.transaction());
        let prev = db_txn.get_meta(DOC_NAME, "key").unwrap();
        db_txn
            .insert_meta(DOC_NAME, "key", "value2".as_bytes())
            .unwrap();
        db_txn.commit().unwrap();
        assert_eq!(prev.as_deref(), Some("value1".as_bytes()));

        let db_txn = RocksDBStore::from(db.transaction());
        let prev = db_txn.get_meta(DOC_NAME, "key").unwrap();
        db_txn.remove_meta(DOC_NAME, "key").unwrap();
        assert_eq!(prev.as_deref(), Some("value2".as_bytes()));
        let value = db_txn.get_meta(DOC_NAME, "key").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn doc_meta_iter() {
        let tmp = TempDir::new("rocksdb-doc_meta_iter").unwrap();
        let db = init_env(&tmp);
        let db_txn = RocksDBStore::from(db.transaction());

        db_txn.insert_meta("A", "key1", [1].as_ref()).unwrap();
        db_txn.insert_meta("B", "key2", [2].as_ref()).unwrap();
        db_txn.insert_meta("B", "key3", [3].as_ref()).unwrap();
        db_txn.insert_meta("C", "key4", [4].as_ref()).unwrap();

        let mut i = db_txn.iter_meta("B").unwrap();
        assert_eq!(i.next(), Some(("key2".as_bytes().into(), [2].into())));
        assert_eq!(i.next(), Some(("key3".as_bytes().into(), [3].into())));
        assert!(i.next().is_none());
    }

    #[test]
    fn doc_iter() {
        let tmp = TempDir::new("rocksdb-doc_iter").unwrap();
        let db = init_env(&tmp);
        let db = Arc::new(db);

        // insert metadata
        {
            let db_txn = RocksDBStore::from(db.transaction());
            db_txn.insert_meta("A", "key1", [1].as_ref()).unwrap();
            db_txn.commit().unwrap();
        }

        // insert full doc state
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello world");

            let db_txn = RocksDBStore::from(db.transaction());
            db_txn.insert_doc("B", &txn).unwrap();
            db_txn.commit().unwrap();
        }

        // insert update
        {
            let doc = Doc::new();
            let db = db.clone();
            let _sub = doc.observe_update_v1(move |_, u| {
                let db_txn = RocksDBStore::from(db.transaction());
                db_txn.push_update("C", &u.update).unwrap();
                db_txn.commit().unwrap();
            });
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello world");
        }

        {
            let db_txn = RocksDBStore::from(db.transaction());
            let mut i = db_txn.iter_docs().unwrap();
            assert_eq!(i.next(), Some("A".as_bytes().into()));
            assert_eq!(i.next(), Some("B".as_bytes().into()));
            assert_eq!(i.next(), Some("C".as_bytes().into()));
            assert!(i.next().is_none());
        }

        // clear doc
        {
            let db_txn = RocksDBStore::from(db.transaction());
            db_txn.clear_doc("B").unwrap();
            db_txn.commit().unwrap();
        }

        {
            let db_txn = RocksDBStore::from(db.transaction());
            let mut i = db_txn.iter_docs().unwrap();
            assert_eq!(i.next(), Some("A".as_bytes().into()));
            assert_eq!(i.next(), Some("C".as_bytes().into()));
            assert!(i.next().is_none());
        }
    }
}
