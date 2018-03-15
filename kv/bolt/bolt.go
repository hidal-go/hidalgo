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

func New(d *bolt.DB) *DB {
	return &DB{DB: d}
}

func Open(path string, opt *bolt.Options) (kv.DB, error) {
	db, err := bolt.Open(path, 0644, opt)
	if err != nil {
		return nil, err
	}
	return &DB{DB: db}, nil
}

type DB struct {
	DB *bolt.DB
}

func (db *DB) Type() string {
	return Type
}

func (db *DB) Close() error {
	return db.DB.Close()
}

func (db *DB) Tx(update bool) (kv.BucketTx, error) {
	tx, err := db.DB.Begin(update)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx}, nil
}

type Tx struct {
	Tx  *bolt.Tx
	err error
}

func (tx *Tx) Get(ctx context.Context, keys []kv.BucketKey) ([][]byte, error) {
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		if b := tx.Tx.Bucket(k.Bucket); b != nil {
			vals[i] = b.Get(k.Key)
		}
	}
	return vals, nil
}

func (tx *Tx) Commit(ctx context.Context) error {
	if tx.err != nil {
		_ = tx.Tx.Rollback()
		return tx.err
	}
	return tx.Tx.Commit()
}
func (tx *Tx) Rollback() error {
	if tx.err != nil {
		_ = tx.Tx.Rollback()
		return tx.err
	}
	return tx.Tx.Rollback()
}
func (tx *Tx) Bucket(name []byte) kv.Bucket {
	if tx.Tx.Writable() {
		b, err := tx.Tx.CreateBucketIfNotExists(name)
		return &Bucket{Bucket: b, err: err}
	}
	b := tx.Tx.Bucket(name)
	var err error
	if b == nil {
		err = kv.ErrNoBucket
	}
	return &Bucket{Bucket: b, err: err}
}

type Bucket struct {
	Bucket *bolt.Bucket
	err    error
}

func (b *Bucket) Get(ctx context.Context, keys [][]byte) ([][]byte, error) {
	if b.err != nil {
		return nil, b.err
	} else if b.Bucket == nil {
		return nil, kv.ErrNotFound
	}
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		vals[i] = b.Bucket.Get(k)
	}
	return vals, nil
}
func (b *Bucket) Put(k, v []byte) error {
	if b.err != nil {
		return b.err
	}
	return b.Bucket.Put(k, v)
}
func (b *Bucket) Del(k []byte) error {
	if b.err != nil {
		return b.err
	}
	return b.Bucket.Delete(k)
}
func (b *Bucket) Scan(pref []byte) kv.Iterator {
	return &Iterator{b: b, pref: pref}
}

type Iterator struct {
	b    *Bucket
	pref []byte
	c    *bolt.Cursor
	k, v []byte
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.b == nil {
		return false
	} else if it.b.err != nil {
		return false
	}
	if it.c == nil {
		it.c = it.b.Bucket.Cursor()
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
func (it *Iterator) Key() []byte { return it.k }
func (it *Iterator) Val() []byte { return it.v }
func (it *Iterator) Err() error {
	if it.b == nil {
		return nil
	}
	return it.b.err
}
func (it *Iterator) Close() error { return it.Err() }

func (b *Bucket) SetFillPercent(v float64) {
	b.Bucket.FillPercent = v
}
