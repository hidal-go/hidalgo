// Package kv provides an abstraction over hierarchical key-value stores.
package kv

import (
	"context"
	"errors"

	"github.com/nwca/uda/base"
)

var (
	// ErrNotFound is returned then a key was not found in the database.
	ErrNotFound = errors.New("kv: not found")
	// ErrReadOnly is returned when write operation is performed on read-only database or transaction.
	ErrReadOnly = errors.New("kv: read only")
)

// KV is an interface for hierarchical key-value databases.
type KV interface {
	base.DB
	Tx(rw bool) (Tx, error)
}

// Key is a hierarchical binary key used in a database.
type Key [][]byte

// Clone returns a copy of the key.
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

// Value is a binary value stored in a database.
type Value []byte

// Clone returns a copy of the value.
func (v Value) Clone() Value {
	if v == nil {
		return nil
	}
	p := make(Value, len(v))
	copy(p, v)
	return p
}

// Pair is a key-value pair.
type Pair struct {
	Key Key
	Val Value
}

// Tx is a transaction over hierarchical key-value store.
type Tx interface {
	base.Tx
	// Get fetches a value for a single key from the database.
	// It return ErrNotFound if key does not exists.
	Get(ctx context.Context, key Key) (Value, error)
	// GetBatch fetches values for multiple keys from the database.
	// Nil element in the slice indicates that key does not exists.
	GetBatch(ctx context.Context, keys []Key) ([]Value, error)
	// Put writes a key-value pair to the database.
	// New value will immediately be visible by Get on the same Tx,
	// but implementation might buffer the write until transaction is committed.
	Put(k Key, v Value) error
	// Del removes the key from the database. See Put for consistency guaranties.
	Del(k Key) error
	// Scan will iterate over all key-value pairs with a specific key prefix.
	Scan(pref Key) Iterator
}

// Iterator is an iterator over hierarchical key-value store.
type Iterator interface {
	base.Iterator
	// Key return current key. The value will become invalid on Next or Close.
	// Caller should not modify or store the value - use Clone.
	Key() Key
	// Key return current value. The value will become invalid on Next or Close.
	// Caller should not modify or store the value - use Clone.
	Val() Value
}
