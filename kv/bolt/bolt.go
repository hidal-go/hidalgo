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
	b, pref := tx.bucket(pref)
	if b == nil || len(pref) != 1 {
		return &Iterator{}
	}
	return &Iterator{b: b, pref: pref[0], kpref: kpref}
}

type Iterator struct {
	b     *bolt.Bucket
	kpref kv.Key
	pref  []byte
	c     *bolt.Cursor
	k, v  []byte
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.b == nil {
		return false
	}
	if it.c == nil {
		it.c = it.b.Cursor()
		if len(it.pref) == 0 {
			it.k, it.v = it.c.First()
		} else {
			it.k, it.v = it.c.Seek(it.pref)
		}
	} else {
		it.k, it.v = it.c.Next()
	}
	ok := it.k != nil && bytes.HasPrefix(it.k, it.pref)
	if !ok {
		it.b = nil
	}
	return ok
}
func (it *Iterator) Key() kv.Key {
	if len(it.kpref) == 0 {
		return kv.Key{it.k}
	}
	k := it.kpref.Clone()
	k = append(k, append([]byte{}, it.k...))
	return k
}
func (it *Iterator) Val() kv.Value { return it.v }
func (it *Iterator) Err() error {
	return nil
}
func (it *Iterator) Close() error {
	*it = Iterator{}
	return nil
}
