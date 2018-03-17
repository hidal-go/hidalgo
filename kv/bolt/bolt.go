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

	"github.com/boltdb/bolt"

	"github.com/nwca/uda/kv"
)

const (
	Type = "bolt"
)

const root = "/"

func New(d *bolt.DB) *DB {
	return &DB{db: d}
}

func Open(path string, opt *bolt.Options) (*DB, error) {
	db, err := bolt.Open(path, 0644, opt)
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

type DB struct {
	db *bolt.DB
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

type Tx struct {
	tx *bolt.Tx
}

func (tx *Tx) bucket(key kv.Key) (*bolt.Bucket, kv.Key) {
	b := tx.tx.Bucket([]byte(root))
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
	for i, k := range keys {
		if b, k := tx.bucket(k); b != nil && len(k) == 1 {
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
	b, err := tx.tx.CreateBucketIfNotExists([]byte(root))
	for err == nil && b != nil && len(k) > 1 {
		b, err = b.CreateBucketIfNotExists(k[0])
		k = k[1:]
	}
	if err != nil {
		return err
	}
	return b.Put(k[0], v)
}
func (tx *Tx) Del(k kv.Key) error {
	b, k := tx.bucket(k)
	if b == nil || len(k) != 1 {
		return nil
	}
	return b.Delete(k[0])
}
func (tx *Tx) Scan(pref kv.Key) kv.Iterator {
	kpref := pref
	b, p := tx.bucket(pref)
	if b == nil || len(p) > 1 {
		// if the prefix key is still longer than 1, it means that
		// a bucket mentioned in the prefix does not exists and
		// we can safely return an empty iterator
		return &Iterator{}
	}
	// the key for bucket we iterate
	kpref = kpref[:len(kpref)-len(p)]
	return &Iterator{
		b:    []*bolt.Bucket{b},
		pref: p,
		root: kpref.Clone(), // we will append to it
	}
}

type Iterator struct {
	root kv.Key // used to reconstruct a full key
	pref kv.Key // prefix to check all keys against
	b    []*bolt.Bucket
	c    []*bolt.Cursor
	k, v []byte
}

func (it *Iterator) Next(ctx context.Context) bool {
	for len(it.b) > 0 {
		i := len(it.b) - 1
		cb := it.b[i]
		if len(it.c) < len(it.b) {
			c := cb.Cursor()
			it.c = append(it.c, c)
			if i >= len(it.pref) {
				it.k, it.v = c.First()
			} else {
				it.k, it.v = c.Seek(it.pref[i])
			}
		} else {
			c := it.c[i]
			it.k, it.v = c.Next()
		}
		if it.k != nil {
			// found a key, check prefix
			if i >= len(it.pref) || bytes.HasPrefix(it.k, it.pref[i]) {
				// prefix matches, or is not specified
				if it.v == nil {
					// it's a bucket
					cb := it.b[len(it.b)-1]
					if b := cb.Bucket(it.k); b != nil {
						it.b = append(it.b, b)
						it.root = append(it.root, it.k)
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
		it.c = it.c[:len(it.c)-1]
		it.b = it.b[:len(it.b)-1]
		if len(it.root) > 0 { // since we hide top-level bucket it can be smaller
			it.root = it.root[:len(it.root)-1]
		}
	}
	return false
}
func (it *Iterator) Key() kv.Key {
	if len(it.b) == 0 {
		return nil
	}
	k := it.root.Clone()
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
