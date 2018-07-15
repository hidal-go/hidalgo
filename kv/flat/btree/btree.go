// Copyright 2017 The Cayley Authors. All rights reserved.
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

package btree

import (
	"bytes"
	"context"
	"io"

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/kv/flat"
)

const (
	Name = "btree"
)

func init() {
	flat.Register(flat.Registration{
		Registration: base.Registration{
			Name: Name, Title: "B-Tree",
			Local: true, Volatile: true,
		},
		OpenPath: func(path string) (flat.KV, error) {
			if path != "" {
				return nil, base.ErrVolatile
			}
			return New(), nil
		},
	})
}

var _ flat.KV = (*DB)(nil)

// New creates a new flat in-memory key-value store.
// It's not safe for concurrent use.
func New() *DB {
	return &DB{t: TreeNew(bytes.Compare)}
}

type DB struct {
	t *Tree
}

func (db *DB) Close() error {
	return nil
}
func (db *DB) Tx(rw bool) (flat.Tx, error) {
	return &Tx{t: db.t, rw: rw}, nil
}

type Tx struct {
	t  *Tree
	rw bool
}

func (tx *Tx) Get(ctx context.Context, key flat.Key) (flat.Value, error) {
	v, ok := tx.t.Get(key)
	if !ok {
		return nil, flat.ErrNotFound
	}
	return flat.Value(v).Clone(), nil
}
func (tx *Tx) GetBatch(ctx context.Context, keys []flat.Key) ([]flat.Value, error) {
	vals := make([]flat.Value, len(keys))
	for i, k := range keys {
		if v, ok := tx.t.Get(k); ok {
			vals[i] = flat.Value(v).Clone()
		}
	}
	return vals, nil
}

func (tx *Tx) Commit(ctx context.Context) error {
	return nil
}
func (tx *Tx) Close() error {
	return nil
}
func (tx *Tx) Put(k flat.Key, v flat.Value) error {
	if !tx.rw {
		return flat.ErrReadOnly
	}
	tx.t.Set(k.Clone(), v.Clone())
	return nil
}
func (tx *Tx) Del(k flat.Key) error {
	if !tx.rw {
		return flat.ErrReadOnly
	}
	tx.t.Delete(k)
	return nil
}
func (tx *Tx) Scan(pref flat.Key) flat.Iterator {
	return &Iterator{t: tx.t, pref: pref}
}

type Iterator struct {
	t    *Tree
	pref []byte
	e    *Enumerator
	k, v []byte
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.t == nil {
		return false
	}
	if it.e == nil {
		it.e, _ = it.t.Seek(it.pref)
	}
	k, v, err := it.e.Next()
	if err == io.EOF {
		return false
	} else if !bytes.HasPrefix(k, it.pref) {
		return false
	}
	it.k, it.v = k, v
	return true
}
func (it *Iterator) Key() flat.Key   { return it.k }
func (it *Iterator) Val() flat.Value { return it.v }
func (it *Iterator) Err() error {
	return nil
}
func (it *Iterator) Close() error {
	if it.e != nil {
		it.e.Close()
		it.e = nil
	}
	return it.Err()
}
