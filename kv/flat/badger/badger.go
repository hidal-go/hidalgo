package badger

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v2"

	"github.com/hidal-go/hidalgo/base"
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

func (db *DB) View(ctx context.Context, fn func(tx flat.Tx) error) error {
	return flat.View(ctx, db, fn)
}

func (db *DB) Update(ctx context.Context, fn func(tx flat.Tx) error) error {
	return flat.Update(ctx, db, fn)
}

type Tx struct {
	tx *badger.Txn
}

func (tx *Tx) Commit(ctx context.Context) error {
	err := tx.tx.Commit()
	if errors.Is(err, badger.ErrConflict) {
		err = flat.ErrConflict
	}
	return err
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
	if errors.Is(err, badger.ErrKeyNotFound) {
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
	err := tx.tx.Set(k, v)
	if errors.Is(err, badger.ErrConflict) {
		err = flat.ErrConflict
	}
	return err
}

func (tx *Tx) Del(k flat.Key) error {
	err := tx.tx.Delete(k)
	if errors.Is(err, badger.ErrConflict) {
		err = flat.ErrConflict
	}
	return err
}

func (tx *Tx) Scan(opts ...flat.IteratorOption) flat.Iterator {
	bit := tx.tx.NewIterator(badger.DefaultIteratorOptions)
	var it flat.Iterator = &Iterator{it: bit, first: true}
	it = flat.ApplyIteratorOptions(it, opts)
	return it
}

var (
	_ flat.Seeker         = &Iterator{}
	_ flat.PrefixIterator = &Iterator{}
)

type Iterator struct {
	err   error
	it    *badger.Iterator
	pref  flat.Key
	first bool
	valid bool
}

func (it *Iterator) Reset() {
	it.first = true
	it.valid = false
	it.err = nil
}

func (it *Iterator) WithPrefix(pref flat.Key) flat.Iterator {
	it.Reset()
	it.pref = pref
	return it
}

func (it *Iterator) next() bool {
	if len(it.pref) != 0 {
		it.valid = it.it.ValidForPrefix(it.pref)
	} else {
		it.valid = it.it.Valid()
	}
	return it.valid
}

func (it *Iterator) Seek(ctx context.Context, key flat.Key) bool {
	it.Reset()
	it.first = false
	it.it.Seek(key)
	return it.next()
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.first {
		it.first = false
		it.it.Seek(it.pref)
	} else {
		it.it.Next()
	}
	return it.next()
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	it.it.Close()
	return it.Err()
}

func (it *Iterator) Key() flat.Key {
	if !it.valid {
		return nil
	}
	ent := it.it.Item()
	if ent == nil {
		return nil
	}
	return ent.Key()
}

func (it *Iterator) Val() flat.Value {
	if !it.valid {
		return nil
	}
	ent := it.it.Item()
	if ent == nil {
		return nil
	}
	v, err := ent.ValueCopy(nil)
	if err != nil {
		it.err = err
	}
	return v
}
