// Copyright 2016 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bolt

import (
	"bytes"
	"context"
	"time"

	"github.com/boltdb/bolt"

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/kv"
)

const (
	Name = "bolt"
)

func init() {
	kv.Register(kv.Registration{
		Registration: base.Registration{
			Name: Name, Title: "BoltDB",
			Local: true,
		},
		OpenPath: OpenPath,
	})
}

var _ kv.KV = (*DB)(nil)

func New(d *bolt.DB) *DB {
	return &DB{db: d}
}

func Open(path string, opt *bolt.Options) (*DB, error) {
	db, err := bolt.Open(path, 0o644, opt)
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

func OpenPath(path string) (kv.KV, error) {
	db, err := Open(path, &bolt.Options{
		Timeout: time.Second,
	})
	if err != nil {
		return nil, err
	}
	return db, nil
}

type DB struct {
	db *bolt.DB
}

func (db *DB) DB() *bolt.DB {
	return db.db
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Tx(rw bool) (kv.Tx, error) {
	tx, err := db.db.Begin(rw)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx}, nil
}

func (db *DB) View(ctx context.Context, fn func(tx kv.Tx) error) error {
	return kv.View(ctx, db, fn)
}

func (db *DB) Update(ctx context.Context, fn func(tx kv.Tx) error) error {
	return kv.Update(ctx, db, fn)
}

type Tx struct {
	tx *bolt.Tx
}

func (tx *Tx) root() *bolt.Bucket {
	// a hack to get the root bucket
	c := tx.tx.Cursor()
	return c.Bucket()
}

func (tx *Tx) bucket(key kv.Key) (*bolt.Bucket, kv.Key) {
	if len(key) <= 1 {
		return tx.root(), key
	}

	b := tx.tx.Bucket(key[0])
	key = key[1:]
	for b != nil && len(key) > 1 {
		b = b.Bucket(key[0])
		key = key[1:]
	}

	return b, key
}

func (tx *Tx) Get(ctx context.Context, key kv.Key) (kv.Value, error) {
	b, k := tx.bucket(key)
	if b == nil || len(k) != 1 {
		return nil, kv.ErrNotFound
	}

	v := b.Get(k[0])
	if v == nil {
		return nil, kv.ErrNotFound
	}

	return v, nil
}

func (tx *Tx) GetBatch(ctx context.Context, keys []kv.Key) ([]kv.Value, error) {
	vals := make([]kv.Value, len(keys))
	for i, key := range keys {
		if b, k := tx.bucket(key); b != nil && len(k) == 1 {
			vals[i] = b.Get(k[0])
		}
	}
	return vals, nil
}

func (tx *Tx) Commit(ctx context.Context) error {
	return tx.tx.Commit()
}

func (tx *Tx) Close() error {
	return tx.tx.Rollback()
}

func (tx *Tx) Put(k kv.Key, v kv.Value) error {
	var (
		b   *bolt.Bucket
		err error
	)

	if len(k) <= 1 {
		b = tx.root()
	} else {
		b, err = tx.tx.CreateBucketIfNotExists(k[0])
		k = k[1:]
	}

	for err == nil && b != nil && len(k) > 1 {
		b, err = b.CreateBucketIfNotExists(k[0])
		k = k[1:]
	}
	if err != nil {
		return err
	} else if len(k[0]) == 0 && len(v) == 0 {
		return nil // bucket creation, no need to put value
	}

	err = b.Put(k[0], v)
	if err == bolt.ErrTxNotWritable {
		err = kv.ErrReadOnly
	}

	return err
}

func (tx *Tx) Del(k kv.Key) error {
	b, k := tx.bucket(k)
	if b == nil || len(k) != 1 {
		return nil
	}

	err := b.Delete(k[0])
	if err == bolt.ErrTxNotWritable {
		err = kv.ErrReadOnly
	}

	return err
}

func (tx *Tx) Scan(opts ...kv.IteratorOption) kv.Iterator {
	var it kv.Iterator = &Iterator{
		tx:    tx,
		rootb: tx.root(),
	}
	it.Reset()
	return kv.ApplyIteratorOptions(it, opts)
}

var (
	_ kv.Seeker         = &Iterator{}
	_ kv.PrefixIterator = &Iterator{}
)

type Iterator struct {
	tx    *Tx // only used for iterator optimization
	rootb *bolt.Bucket
	rootk kv.Key // used to reconstruct a full key
	pref  kv.Key // prefix to check all keys against
	stack struct {
		k kv.Key
		b []*bolt.Bucket
		c []*bolt.Cursor
	}
	k, v []byte // inside the current bucket
}

func (it *Iterator) Reset() {
	it.k = nil
	it.v = nil
	it.stack.c = nil
	it.stack.b = nil

	if cap(it.stack.k) >= len(it.rootk) {
		it.stack.k = it.stack.k[:len(it.rootk)]
	} else {
		// we will append to it
		it.stack.k = it.rootk.Clone()
	}

	copy(it.stack.k, it.rootk)

	if it.rootb != nil {
		it.stack.b = []*bolt.Bucket{it.rootb}
	}
}

func (it *Iterator) WithPrefix(pref kv.Key) kv.Iterator {
	it.Reset()

	kpref := pref
	b, p := it.tx.bucket(pref)
	if b == nil || len(p) > 1 {
		// if the prefix key is still longer than 1, it means that
		// a bucket mentioned in the prefix does not exists and
		// we can safely return an empty iterator
		*it = Iterator{tx: it.tx}
		return it
	}

	// the key for bucket we iterate
	it.rootk = kpref[:len(kpref)-len(p)]
	it.rootb = b
	it.pref = p
	it.Reset()
	return it
}

func (it *Iterator) next(pref kv.Key) bool {
	for len(it.stack.b) > 0 {
		i := len(it.stack.b) - 1
		cb := it.stack.b[i]

		if len(it.stack.c) < len(it.stack.b) {
			c := cb.Cursor()
			it.stack.c = append(it.stack.c, c)
			if i >= len(pref) {
				it.k, it.v = c.First()
			} else {
				it.k, it.v = c.Seek(pref[i])
			}
		} else {
			c := it.stack.c[i]
			it.k, it.v = c.Next()
		}

		if it.k != nil {
			// found a key, check prefix
			if i >= len(pref) || bytes.HasPrefix(it.k, pref[i]) {
				// prefix matches, or is not specified
				if it.v == nil {
					// it's a bucket
					cb := it.stack.b[len(it.stack.b)-1]
					if b := cb.Bucket(it.k); b != nil {
						it.stack.b = append(it.stack.b, b)
						it.stack.k = append(it.stack.k, it.k)
						continue
					}
					// or maybe it's a key after all
				}
				// return this value
				return true
			}
		}

		// iterator is ended, or we reached the end of the prefix
		// return to top-level bucket
		it.stack.c = it.stack.c[:len(it.stack.c)-1]
		it.stack.b = it.stack.b[:len(it.stack.b)-1]
		if len(it.stack.k) > 0 { // since we hide top-level bucket it can be smaller
			it.stack.k = it.stack.k[:len(it.stack.k)-1]
		}
	}

	return false
}

func (it *Iterator) Seek(ctx context.Context, key kv.Key) bool {
	it.Reset()
	if !it.next(key) {
		return false
	}
	return it.Key().HasPrefix(it.pref)
}

func (it *Iterator) Next(ctx context.Context) bool {
	return it.next(it.pref)
}

func (it *Iterator) Key() kv.Key {
	if len(it.stack.b) == 0 {
		return nil
	}
	k := it.stack.k.Clone()
	k = append(k, append([]byte{}, it.k...))
	return k
}

func (it *Iterator) Val() kv.Value {
	return it.v
}

func (it *Iterator) Err() error {
	return nil
}

func (it *Iterator) Close() error {
	*it = Iterator{}
	return nil
}
