pub mod error;
pub mod keys;

use crate::error::Error;
use crate::keys::{
    doc_oid_name, key_doc, key_doc_end, key_doc_start, key_meta, key_meta_end, key_meta_start,
    key_oid, key_state_vector, key_update, Key, KEYSPACE_DOC, KEYSPACE_OID, OID, V1,
};
use std::convert::TryInto;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, StateVector, Transact, TransactionMut, Update};

/// A trait to be implemented by the specific key-value store transaction equivalent in order to
/// auto-implement features provided by [DocOps] trait.
pub trait KVStore<'a> {
    /// Error type returned from the implementation.
    type Error: std::error::Error;
    /// Cursor type used to iterate over the ordered range of key-value entries.
    type Cursor: Iterator<Item = Self::Entry>;
    /// Entry type returned by cursor.
    type Entry: KVEntry;
    /// Type returned from the implementation. Different key-value stores have different
    /// abstractions over the binary data they use.
    type Return: AsRef<[u8]>;

    /// Return a value stored under given `key` or `None` if key was not found.
    fn get(&self, key: &[u8]) -> Result<Option<Self::Return>, Self::Error>;

    /// Insert a new `value` under given `key` or replace an existing value with new one if
    /// entry with that `key` already existed.
    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    /// Return a value stored under the given `key` if it exists.
    fn remove(&self, key: &[u8]) -> Result<(), Self::Error>;

    /// Remove all keys between `from`..=`to` range of keys.
    fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error>;

    /// Return an iterator over all entries between `from`..=`to` range of keys.
    fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error>;

    /// Looks into the last entry value prior to a given key. The provided key parameter may not
    /// exist and it's used only to establish cursor position in ordered key collection.
    ///
    /// In example: in a key collection of `{1,2,5,7}`, this method with the key parameter of `4`
    /// should return value of `2`.
    fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error>;
}

pub trait KVEntry {
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
}

/// Trait used to automatically implement core operations over the Yrs document.
pub trait DocOps<'a>: KVStore<'a> + Sized
where
    Error: From<<Self as KVStore<'a>>::Error>,
{
    /// Inserts or updates a document given it's read transaction and name. lib0 v1 encoding is
    /// used for storing the document.
    ///
    /// This feature requires a write capabilities from the database transaction.
    fn insert_doc<K: AsRef<[u8]> + ?Sized, T: ReadTxn>(
        &self,
        name: &K,
        txn: &T,
    ) -> Result<(), Error> {
        let doc_state = txn.encode_diff_v1(&StateVector::default());
        let state_vector = txn.state_vector().encode_v1();
        self.insert_doc_raw_v1(name.as_ref(), &doc_state, &state_vector)
    }

    /// Inserts or updates a document given it's binary update and state vector. lib0 v1 encoding is
    /// assumed as a format for storing the document.
    ///
    /// This is useful when you i.e. want to pre-serialize big document prior to acquiring
    /// a database transaction.
    ///
    /// This feature requires a write capabilities from the database transaction.
    fn insert_doc_raw_v1(
        &self,
        name: &[u8],
        doc_state_v1: &[u8],
        doc_sv_v1: &[u8],
    ) -> Result<(), Error> {
        let oid = get_or_create_oid(self, name)?;
        insert_inner_v1(self, oid, doc_state_v1, doc_sv_v1)?;
        Ok(())
    }

    /// Loads the document state stored in current database under given document `name` into
    /// in-memory Yrs document using provided [TransactionMut]. This includes potential update
    /// entries that may not have been merged with the main document state yet.
    ///
    /// This feature requires only a read capabilities from the database transaction.
    fn load_doc<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
        txn: &mut TransactionMut,
    ) -> Result<bool, Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let loaded = load_doc(self, oid, txn)?;
            Ok(loaded != 0)
        } else {
            Ok(false)
        }
    }

    /// Merges all updates stored via [Self::push_update] that were detached from the main document
    /// state, updates the document and its state vector and finally prunes the updates that have
    /// been integrated this way. Returns the [Doc] with the most recent state produced this way.
    ///
    /// This feature requires a write capabilities from the database transaction.
    fn flush_doc<K: AsRef<[u8]> + ?Sized>(&self, name: &K) -> Result<Option<Doc>, Error> {
        self.flush_doc_with(name, yrs::Options::default())
    }

    /// Merges all updates stored via [Self::push_update] that were detached from the main document
    /// state, updates the document and its state vector and finally prunes the updates that have
    /// been integrated this way. `options` are used to drive the details of integration process.
    /// Returns the [Doc] with the most recent state produced this way, initialized using
    /// `options` parameter.
    ///
    /// This feature requires a write capabilities from the database transaction.
    fn flush_doc_with<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
        options: yrs::Options,
    ) -> Result<Option<Doc>, Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let doc = flush_doc(self, oid, options)?;
            Ok(doc)
        } else {
            Ok(None)
        }
    }

    /// Returns the [StateVector] stored directly for the document with a given `name`.
    /// Returns `None` if the state vector was not stored.
    ///
    /// Keep in mind that this method only returns a state vector that's stored directly. A second
    /// tuple parameter boolean informs if returned value is up to date. If that's not the case, it
    /// means that state vector exists but must be recalculated from the collection of persisted
    /// updates using either [Self::load_doc] (read-only) or [Self::flush_doc] (read-write).
    ///
    /// This feature requires only the read capabilities from the database transaction.
    fn get_state_vector<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
    ) -> Result<(Option<StateVector>, bool), Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let key = key_state_vector(oid);
            let data = self.get(&key)?;
            let sv = if let Some(data) = data {
                let state_vector = StateVector::decode_v1(data.as_ref())?;
                Some(state_vector)
            } else {
                None
            };
            let update_range_start = key_update(oid, 0);
            let update_range_end = key_update(oid, u32::MAX);
            let mut iter = self.iter_range(&update_range_start, &update_range_end)?;
            let up_to_date = iter.next().is_none();
            Ok((sv, up_to_date))
        } else {
            Ok((None, true))
        }
    }

    /// Appends new update without integrating it directly into document store (which is faster
    /// than persisting full document state on every update). Updates are assumed to be serialized
    /// using lib0 v1 encoding.
    ///
    /// Returns a sequence number of a stored update. Once updates are integrated into document and
    /// pruned (using [Self::flush_doc] method), sequence number is reset.
    ///
    /// This feature requires a write capabilities from the database transaction.
    fn push_update<K: AsRef<[u8]> + ?Sized>(&self, name: &K, update: &[u8]) -> Result<u32, Error> {
        let oid = get_or_create_oid(self, name.as_ref())?;
        let last_clock = {
            let end = key_update(oid, u32::MAX);
            if let Some(e) = self.peek_back(&end)? {
                let last_key = e.key();
                let len = last_key.len();
                let last_clock = &last_key[(len - 5)..(len - 1)]; // update key scheme: 01{name:n}1{clock:4}0
                u32::from_be_bytes(last_clock.try_into().unwrap())
            } else {
                0
            }
        };
        let clock = last_clock + 1;
        let update_key = key_update(oid, clock);
        self.upsert(&update_key, &update)?;
        Ok(clock)
    }

    /// Returns an update (encoded using lib0 v1 encoding) which contains all new changes that
    /// happened since provided state vector for a given document.
    ///
    /// This feature requires only the read capabilities from the database transaction.
    fn get_diff<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
        sv: &StateVector,
    ) -> Result<Option<Vec<u8>>, Error> {
        let doc = Doc::new();
        let found = {
            let mut txn = doc.transact_mut();
            self.load_doc(name, &mut txn)?
        };
        if found {
            Ok(Some(doc.transact().encode_diff_v1(sv)))
        } else {
            Ok(None)
        }
    }

    /// Removes all data associated with the current document (including its updates and metadata).
    ///
    /// This feature requires a write capabilities from the database transaction.
    fn clear_doc<K: AsRef<[u8]> + ?Sized>(&self, name: &K) -> Result<(), Error> {
        let oid_key = key_oid(name.as_ref());
        if let Some(oid) = self.get(&oid_key)? {
            // all document related elements are stored within bounds [0,1,..oid,0]..[0,1,..oid,255]
            let oid: [u8; 4] = oid.as_ref().try_into().unwrap();
            let oid = OID::from_be_bytes(oid);
            self.remove(&oid_key)?;
            let start = key_doc_start(oid);
            let end = key_doc_end(oid);
            for v in self.iter_range(&start, &end)? {
                let key: &[u8] = v.key();
                if key > &end {
                    break; //TODO: for some reason key range doesn't always work
                }
                self.remove(&key)?;
            }
        }
        Ok(())
    }

    /// Returns a metadata value stored under its metadata `key` for a document with given `name`.
    ///
    /// This feature requires only the read capabilities from the database transaction.
    fn get_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K1,
        meta_key: &K2,
    ) -> Result<Option<Self::Return>, Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let key = key_meta(oid, meta_key.as_ref());
            Ok(self.get(&key)?)
        } else {
            Ok(None)
        }
    }

    /// Inserts or updates new `meta` value stored under its metadata `key` for a document with
    /// given `name`.
    ///
    /// This feature requires write capabilities from the database transaction.
    fn insert_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K1,
        meta_key: &K2,
        meta: &[u8],
    ) -> Result<(), Error> {
        let oid = get_or_create_oid(self, name.as_ref())?;
        let key = key_meta(oid, meta_key.as_ref());
        self.upsert(&key, meta)?;
        Ok(())
    }

    /// Removes an metadata entry stored under given metadata `key` for a document with provided `name`.
    ///
    /// This feature requires write capabilities from the database transaction.
    fn remove_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K1,
        meta_key: &K2,
    ) -> Result<(), Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let key = key_meta(oid, meta_key.as_ref());
            self.remove(&key)?;
        }
        Ok(())
    }

    /// Returns an iterator over all document names stored in current database.
    fn iter_docs(&self) -> Result<DocsNameIter<Self::Cursor, Self::Entry>, Error> {
        let start = Key::from_const([V1, KEYSPACE_OID]);
        let end = Key::from_const([V1, KEYSPACE_DOC]);
        let cursor = self.iter_range(&start, &end)?;
        Ok(DocsNameIter { cursor, start, end })
    }

    /// Returns an iterator over all metadata entries stored for a given document.
    fn iter_meta<K: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K,
    ) -> Result<MetadataIter<Self::Cursor, Self::Entry>, Error> {
        if let Some(oid) = get_oid(self, doc_name.as_ref())? {
            let start = key_meta_start(oid).to_vec();
            let end = key_meta_end(oid).to_vec();
            let cursor = self.iter_range(&start, &end)?;
            Ok(MetadataIter(Some((cursor, start, end))))
        } else {
            Ok(MetadataIter(None))
        }
    }
}

fn get_oid<'a, DB: DocOps<'a> + ?Sized>(db: &DB, name: &[u8]) -> Result<Option<OID>, Error>
where
    Error: From<<DB as KVStore<'a>>::Error>,
{
    let key = key_oid(name);
    let value = db.get(&key)?;
    if let Some(value) = value {
        let bytes: [u8; 4] = value.as_ref().try_into().unwrap();
        let oid = OID::from_be_bytes(bytes);
        Ok(Some(oid))
    } else {
        Ok(None)
    }
}

fn get_or_create_oid<'a, DB: DocOps<'a> + ?Sized>(db: &DB, name: &[u8]) -> Result<OID, Error>
where
    Error: From<<DB as KVStore<'a>>::Error>,
{
    if let Some(oid) = get_oid(db, name)? {
        Ok(oid)
    } else {
        /*
           Since pattern is:

           00{doc_name:n}0      - OID key pattern
           01{oid:4}0           - document key pattern

           Use 00{0000}0 to try to move cursor to GTE first document, then move cursor 1 position
           back to get the latest OID or not found.
        */
        let last_oid = if let Some(e) = db.peek_back([V1, KEYSPACE_DOC].as_ref())? {
            let value = e.value();
            let last_value = OID::from_be_bytes(value.try_into().unwrap());
            last_value
        } else {
            0
        };
        let new_oid = last_oid + 1;
        let key = key_oid(name);
        db.upsert(&key, new_oid.to_be_bytes().as_ref())?;
        Ok(new_oid)
    }
}

fn load_doc<'a, DB: DocOps<'a> + ?Sized>(
    db: &DB,
    oid: OID,
    txn: &mut TransactionMut,
) -> Result<u32, Error>
where
    Error: From<<DB as KVStore<'a>>::Error>,
{
    let mut found = false;
    {
        let doc_key = key_doc(oid);
        if let Some(doc_state) = db.get(&doc_key)? {
            let update = Update::decode_v1(doc_state.as_ref())?;
            txn.apply_update(update);
            found = true;
        }
    }
    let mut update_count = 0;
    {
        let update_key_start = key_update(oid, 0);
        let update_key_end = key_update(oid, u32::MAX);
        let mut iter = db.iter_range(&update_key_start, &update_key_end)?;
        while let Some(e) = iter.next() {
            let value = e.value();
            let update = Update::decode_v1(value)?;
            txn.apply_update(update);
            update_count += 1;
        }
    }
    if found {
        update_count |= 1 << 31; // mark hi bit to note that document core state was used
    }
    Ok(update_count)
}

fn delete_updates<'a, DB: DocOps<'a> + ?Sized>(db: &DB, oid: OID) -> Result<(), Error>
where
    Error: From<<DB as KVStore<'a>>::Error>,
{
    let start = key_update(oid, 0);
    let end = key_update(oid, u32::MAX);
    db.remove_range(&start, &end)?;
    Ok(())
}

fn flush_doc<'a, DB: DocOps<'a> + ?Sized>(
    db: &DB,
    oid: OID,
    options: yrs::Options,
) -> Result<Option<Doc>, Error>
where
    Error: From<<DB as KVStore<'a>>::Error>,
{
    let doc = Doc::with_options(options);
    let found = load_doc(db, oid, &mut doc.transact_mut())?;
    if found & !(1 << 31) != 0 {
        // loaded doc was generated from updates
        let txn = doc.transact();
        let doc_state = txn.encode_state_as_update_v1(&StateVector::default());
        let state_vec = txn.state_vector().encode_v1();
        drop(txn);

        insert_inner_v1(db, oid, &doc_state, &state_vec)?;
        delete_updates(db, oid)?;
        Ok(Some(doc))
    } else {
        Ok(None)
    }
}

fn insert_inner_v1<'a, DB: DocOps<'a> + ?Sized>(
    db: &DB,
    oid: OID,
    doc_state_v1: &[u8],
    doc_sv_v1: &[u8],
) -> Result<(), Error>
where
    error::Error: From<<DB as KVStore<'a>>::Error>,
{
    let key_doc = key_doc(oid);
    let key_sv = key_state_vector(oid);
    db.upsert(&key_doc, doc_state_v1)?;
    db.upsert(&key_sv, doc_sv_v1)?;
    Ok(())
}

pub struct DocsNameIter<I, E>
where
    I: Iterator<Item = E>,
    E: KVEntry,
{
    cursor: I,
    start: Key<2>,
    end: Key<2>,
}

impl<I, E> Iterator for DocsNameIter<I, E>
where
    I: Iterator<Item = E>,
    E: KVEntry,
{
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let e = self.cursor.next()?;
        Some(doc_oid_name(e.key()).into())
    }
}

pub struct MetadataIter<I, E>(Option<(I, Vec<u8>, Vec<u8>)>)
where
    I: Iterator<Item = E>,
    E: KVEntry;

impl<I, E> Iterator for MetadataIter<I, E>
where
    I: Iterator<Item = E>,
    E: KVEntry,
{
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        let (cursor, _, _) = self.0.as_mut()?;
        let v = cursor.next()?;
        let key = v.key();
        let value = v.value();
        let meta_key = &key[7..key.len() - 1];
        Some((meta_key.into(), value.into()))
    }
}
