//go:build !386 && !arm

// Pebble doesn't support 32bit

package pebble

import (
	"bytes"
	"context"

	"github.com/cockroachdb/pebble"

	"github.com/hidal-go/hidalgo/base"
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

// OpenPathOptions is similar to OpenPath, but allow customizing Pebble options.
func OpenPathOptions(path string, opts *pebble.Options) (*DB, error) {
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

func OpenPath(path string) (flat.KV, error) {
	db, err := OpenPathOptions(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return db, nil
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

func (db *DB) Tx(ctx context.Context, rw bool) (flat.Tx, error) {
	return &Tx{tx: db.db.NewIndexedBatch(), rw: rw}, nil
}

func (db *DB) View(ctx context.Context, fn func(tx flat.Tx) error) error {
	return flat.View(ctx, db, fn)
}

func (db *DB) Update(ctx context.Context, fn func(tx flat.Tx) error) error {
	return flat.Update(ctx, db, fn)
}

type Tx struct {
	tx *pebble.Batch
	rw bool
}

func (tx *Tx) Commit(ctx context.Context) error {
	if !tx.rw {
		return flat.ErrReadOnly
	}
	return tx.tx.Commit(pebble.Sync)
}

func (tx *Tx) Close() error {
	return tx.tx.Close()
}

func (tx *Tx) Get(ctx context.Context, key flat.Key) (flat.Value, error) {
	if len(key) == 0 {
		return nil, flat.ErrNotFound
	}
	val, closer, err := tx.tx.Get(key)
	if err == pebble.ErrNotFound {
		return nil, flat.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	defer closer.Close()

	ret := make([]byte, len(val))
	copy(ret, val)
	return ret, nil
}

func (tx *Tx) GetBatch(ctx context.Context, keys []flat.Key) ([]flat.Value, error) {
	return flat.GetBatch(ctx, tx, keys)
}

func (tx *Tx) Put(ctx context.Context, k flat.Key, v flat.Value) error {
	if !tx.rw {
		return flat.ErrReadOnly
	}
	return tx.tx.Set(k, v, pebble.Sync)
}

func (tx *Tx) Del(ctx context.Context, k flat.Key) error {
	if !tx.rw {
		return flat.ErrReadOnly
	}
	return tx.tx.Delete(k, pebble.Sync)
}

func (tx *Tx) Scan(ctx context.Context, opts ...flat.IteratorOption) flat.Iterator {
	pit := tx.tx.NewIter(nil)
	var it flat.Iterator = &Iterator{it: pit, first: true}
	it = flat.ApplyIteratorOptions(it, opts)
	return it
}

var (
	_ flat.Seeker         = &Iterator{}
	_ flat.PrefixIterator = &Iterator{}
)

type Iterator struct {
	it    *pebble.Iterator
	pref  flat.Key
	first bool
	err   error
}

func (it *Iterator) Reset() {
	it.first = true
	it.err = nil
}

func (it *Iterator) WithPrefix(pref flat.Key) flat.Iterator {
	it.Reset()
	it.pref = pref
	return it
}

func (it *Iterator) Seek(ctx context.Context, key flat.Key) bool {
	it.Reset()
	it.first = false
	it.it.SeekGE(key)
	return it.isValid()
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

func (it *Iterator) Val() flat.Value {
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
