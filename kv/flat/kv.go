package flat

import (
	"context"

	"github.com/nwca/uda/base"
	"github.com/nwca/uda/kv"
)

var (
	ErrNotFound = kv.ErrNotFound
	ErrReadOnly = kv.ErrReadOnly
)

type KV interface {
	base.DB
	Tx(rw bool) (Tx, error)
}

type Key []byte

func (k Key) Clone() Key {
	if k == nil {
		return nil
	}
	p := make(Key, len(k))
	copy(p, k)
	return p
}

type Value = kv.Value

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
	Val() kv.Value
}
