package pebble

import (
	"bytes"
	"context"

	"github.com/cockroachdb/pebble"

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
)

const (
	Name = "pebble"
)

func init() {
	flat.Register(flat.Registration{
		Registration: base.Registration{
			Name: Name, Title: "Pebble",
			Local: true,
		},
		OpenPath: OpenPath,
	})
}

var _ flat.KV = (*DB)(nil)

func New(d *pebble.DB) *DB {
	return &DB{db: d}
}

func OpenPath(path string) (flat.KV, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

type DB struct {
	db     *pebble.DB
	closed bool
}

func (db *DB) DB() *pebble.DB {
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
	return &Tx{tx: db.db.NewIndexedBatch()}, nil
}

type Tx struct {
	tx *pebble.Batch
}

func (tx *Tx) Commit(ctx context.Context) error {
	return tx.tx.Commit(nil)
}

func (tx *Tx) Close() error {
	return tx.tx.Close()
}

func (tx *Tx) Get(ctx context.Context, key flat.Key) (flat.Value, error) {
	if len(key) == 0 {
		return nil, flat.ErrNotFound
	}
	found, closer, err := tx.tx.Get(key)
	if err == pebble.ErrNotFound {
		return nil, flat.ErrNotFound
	} else if err != nil {
		return nil, err
	}

	ret := make([]byte, len(found))
	copy(ret, found)
	closer.Close()
	return ret, nil
}

func (tx *Tx) GetBatch(ctx context.Context, keys []flat.Key) ([]flat.Value, error) {
	return flat.GetBatch(ctx, tx, keys)
}

func (tx *Tx) Put(k flat.Key, v flat.Value) error {
	return tx.tx.Set(k, v, pebble.Sync)
}

func (tx *Tx) Del(k flat.Key) error {
	return tx.tx.Delete(k, pebble.Sync)
}

func (tx *Tx) Scan(pref flat.Key) flat.Iterator {
	it := tx.tx.NewIter(nil)
	return &Iterator{it: it, pref: pref, first: true}
}

type Iterator struct {
	it    *pebble.Iterator
	pref  flat.Key
	first bool
	err   error
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.first {
		it.first = false
		it.it.SeekGE(it.pref)
	} else {
		it.it.Next()
	}

	return it.isValid()
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	it.it.Close()
	return it.Err()
}

func (it *Iterator) Key() flat.Key {
	return it.it.Key()
}

func (it *Iterator) Val() kv.Value {
	return it.it.Value()
}

func (it *Iterator) isValid() bool {
	if !it.it.Valid() {
		return false
	}

	if len(it.pref) != 0 && !bytes.HasPrefix(it.it.Key(), it.pref) {
		return false
	}

	return true
}
