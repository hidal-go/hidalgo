package badger

import (
	"context"

	"github.com/dgraph-io/badger/v2"

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
)

const (
	Name = "badger"
)

func init() {
	flat.Register(flat.Registration{
		Registration: base.Registration{
			Name: Name, Title: "Badger",
			Local: true,
		},
		OpenPath: OpenPath,
	})
}

var _ flat.KV = (*DB)(nil)

func New(d *badger.DB) *DB {
	return &DB{db: d}
}

func Open(opt badger.Options) (*DB, error) {
	if opt.ValueDir == "" {
		opt.ValueDir = opt.Dir
	}
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

func OpenPath(path string) (flat.KV, error) {
	opt := badger.DefaultOptions(path)
	db, err := Open(opt)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type DB struct {
	db     *badger.DB
	closed bool
}

func (db *DB) DB() *badger.DB {
	return db.db
}

func (db *DB) Close() error {
	if db.closed {
		return nil
	}
	db.closed = true
	return db.db.Close()
}

func (db *DB) Tx(rw bool) (flat.Tx, error) {
	tx := db.db.NewTransaction(rw)
	return &Tx{tx: tx}, nil
}

type Tx struct {
	tx *badger.Txn
}

func (tx *Tx) Commit(ctx context.Context) error {
	return tx.tx.Commit()
}

func (tx *Tx) Close() error {
	tx.tx.Discard()
	return nil
}

func (tx *Tx) Get(ctx context.Context, key flat.Key) (flat.Value, error) {
	if len(key) == 0 {
		return nil, flat.ErrNotFound
	}
	item, err := tx.tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, flat.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (tx *Tx) GetBatch(ctx context.Context, keys []flat.Key) ([]flat.Value, error) {
	return flat.GetBatch(ctx, tx, keys)
}

func (tx *Tx) Put(k flat.Key, v flat.Value) error {
	return tx.tx.Set(k, v)
}

func (tx *Tx) Del(k flat.Key) error {
	return tx.tx.Delete(k)
}

func (tx *Tx) Scan(pref flat.Key) flat.Iterator {
	it := tx.tx.NewIterator(badger.DefaultIteratorOptions)
	return &Iterator{it: it, pref: pref, first: true}
}

type Iterator struct {
	it    *badger.Iterator
	pref  flat.Key
	first bool
	err   error
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.first {
		it.first = false
		it.it.Seek(it.pref)
	} else {
		it.it.Next()
	}
	if len(it.pref) != 0 {
		return it.it.ValidForPrefix(it.pref)
	}
	return it.it.Valid()
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	it.it.Close()
	return it.Err()
}

func (it *Iterator) Key() flat.Key {
	return it.it.Item().Key()
}

func (it *Iterator) Val() kv.Value {
	v, err := it.it.Item().ValueCopy(nil)
	if err != nil {
		it.err = err
	}
	return v
}
