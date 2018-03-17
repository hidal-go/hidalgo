package kv

import (
	"context"
	"errors"

	"github.com/nwca/uda/base"
)

var (
	ErrNotFound = errors.New("kv: not found")
	ErrReadOnly = errors.New("kv: read only")
)

type KV interface {
	base.DB
	Tx(rw bool) (Tx, error)
}

type Key [][]byte

func (k Key) Clone() Key {
	if k == nil {
		return nil
	}
	p := make(Key, len(k))
	for i, sk := range k {
		p[i] = make([]byte, len(sk))
		copy(p[i], sk)
	}
	return p
}

type Value []byte

func (v Value) Clone() Value {
	if v == nil {
		return nil
	}
	p := make(Value, len(v))
	copy(p, v)
	return p
}

type Pair struct {
	Key Key
	Val Value
}

type Tx interface {
	base.Tx
	// Get fetches a value from a single key from the database.
	// It return ErrNotFound if key does not exists.
	Get(ctx context.Context, key Key) (Value, error)
	// GetBatch fetches values for multiple keys from the database.
	// Nil element in the slice indicates that key does not exists.
	GetBatch(ctx context.Context, keys []Key) ([]Value, error)
	// Put writes a key-value pair to the database.
	// New value will immediately be visible by Get, on the same Tx,
	// but implementation might buffer the write until transaction is committed.
	Put(k Key, v Value) error
	// Del removes the key from database. See Put for consistency guaranties.
	Del(k Key) error
	// Scan will iterate all key-value pairs that has a specific prefix.
	Scan(pref Key) Iterator
}

type Iterator interface {
	base.Iterator
	Key() Key
	Val() Value
}
