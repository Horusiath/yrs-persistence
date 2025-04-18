use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rocksdb::TransactionDB;
use yrs_rocksdb::store::yrs::encoding::read::{Cursor, Read};
use yrs_rocksdb::store::yrs::{uuid_v4, Doc, Text, Transact};

use yrs_kvstore::DocOps;
use yrs_rocksdb::RocksDBStore;

fn bench(c: &mut Criterion) {
    insert_doc(c);
    updates(c);
}

fn insert_doc(c: &mut Criterion) {
    let doc = Doc::new();
    let ops = read_input("editing-trace.bin");
    apply_ops(&doc, ops);

    let clean = Cleaner::new("insert-doc-rocksdb");
    let db = init_env(clean.dir());

    c.bench_with_input(
        BenchmarkId::new("insert document", 1),
        &(doc, db),
        |b, (doc, db)| {
            b.iter(|| {
                let name = uuid_v4().to_string();
                let db_txn = RocksDBStore::from(db.transaction());
                db_txn.insert_doc(&name, &doc.transact()).unwrap();
                db_txn.commit().unwrap();
            });
        },
    );
}

fn updates(c: &mut Criterion) {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("text");

    let ops = read_input("editing-trace.bin");

    let clean = Cleaner::new("insert-doc-lmdb");
    let db = Arc::new(init_env(clean.dir()));

    c.bench_with_input(
        BenchmarkId::new("insert document", ops.len()),
        &(doc, text, ops, db),
        |b, (doc, text, ops, db)| {
            b.iter(|| {
                let db = db.clone();
                let name = uuid_v4().to_string();
                let _sub = doc.observe_update_v1(move |_, e| {
                    let db_txn = RocksDBStore::from(db.transaction());
                    db_txn.push_update(&name, &e.update).unwrap();
                    db_txn.commit().unwrap();
                });

                for op in ops.iter() {
                    let mut txn = doc.transact_mut();
                    match op {
                        TextOp::Insert(idx, txt) => text.insert(&mut txn, *idx, txt),
                        TextOp::Delete(idx, len) => text.remove_range(&mut txn, *idx, *len),
                    }
                }
            });
        },
    );
}

struct Cleaner(&'static str);

impl Cleaner {
    fn new(dir: &'static str) -> Self {
        Self::cleanup(dir);
        Cleaner(dir)
    }

    fn dir(&self) -> &str {
        self.0
    }

    fn cleanup(dir: &str) {
        if let Err(_) = std::fs::remove_dir_all(dir) {
            // if dir doesn't exists, ignore
        }
    }
}

impl Drop for Cleaner {
    fn drop(&mut self) {
        Self::cleanup(self.dir());
    }
}

fn init_env(dir: &str) -> TransactionDB {
    let db = TransactionDB::open_default(dir).unwrap();
    db
}

#[derive(Clone)]
enum TextOp {
    Insert(u32, String),
    Delete(u32, u32),
}

fn apply_ops(doc: &Doc, ops: Vec<TextOp>) {
    let text = doc.get_or_insert_text("text");
    for op in ops.iter() {
        let mut txn = doc.transact_mut();
        match op {
            TextOp::Insert(idx, txt) => text.insert(&mut txn, *idx, txt),
            TextOp::Delete(idx, len) => text.remove_range(&mut txn, *idx, *len),
        }
    }
}

fn read_input(fpath: &str) -> Vec<TextOp> {
    use std::fs::File;
    use yrs_rocksdb::store::yrs::updates::decoder::DecoderV1;

    let mut f = File::open(fpath).unwrap();
    let mut buf = Vec::new();
    std::io::Read::read_to_end(&mut f, &mut buf).unwrap();
    let mut decoder = DecoderV1::new(Cursor::new(buf.as_slice()));
    let len: usize = decoder.read_var().unwrap();
    let mut result = Vec::with_capacity(len);
    for _ in 0..len {
        let op = {
            match decoder.read_var().unwrap() {
                1u32 => {
                    let idx = decoder.read_var().unwrap();
                    let chunk = decoder.read_string().unwrap();
                    TextOp::Insert(idx, chunk.to_string())
                }
                2u32 => {
                    let idx = decoder.read_var().unwrap();
                    let len = decoder.read_var().unwrap();
                    TextOp::Delete(idx, len)
                }
                other => panic!("unrecognized TextOp tag type: {}", other),
            }
        };
        result.push(op);
    }
    result
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench,
}
criterion_main!(benches);
