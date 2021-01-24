// Package flat provides an abstraction over flat key-value stores.
package flat

import (
	"context"
	"fmt"

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/kv"
)

var (
	// ErrNotFound is returned then a key was not found in the database.
	ErrNotFound = kv.ErrNotFound
	// ErrReadOnly is returned when write operation is performed on read-only database or transaction.
	ErrReadOnly = kv.ErrReadOnly
)

// KV is an interface for flat key-value databases.
type KV interface {
	base.DB
	Tx(rw bool) (Tx, error)
	View(func(tx Tx) error) error
	Update(func(tx Tx) error) error
}

// Key is a flat binary key used in a database.
type Key []byte

// Clone returns a copy of the key.
func (k Key) Clone() Key {
	if k == nil {
		return nil
	}
	p := make(Key, len(k))
	copy(p, k)
	return p
}

// Value is a binary value stored in a database.
type Value = kv.Value

// Pair is a key-value pair.
type Pair struct {
	Key Key
	Val Value
}

func (p Pair) String() string {
	return fmt.Sprintf("%x = %x", p.Key, p.Val)
}

type Getter interface {
	// Get fetches a value for a single key from the database.
	// It return ErrNotFound if key does not exists.
	Get(ctx context.Context, key Key) (Value, error)
}

// Tx is a transaction over flat key-value store.
type Tx interface {
	base.Tx
	Getter
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

// Iterator is an iterator over flat key-value store.
type Iterator interface {
	base.Iterator
	// Key return current key. The value will become invalid on Next or Close.
	// Caller should not modify or store the value - use Clone.
	Key() Key
	// Key return current value. The value will become invalid on Next or Close.
	// Caller should not modify or store the value - use Clone.
	Val() Value
}

// GetBatch is an implementation of Tx.GetBatch for databases that has no native implementation for it.
func GetBatch(ctx context.Context, tx Getter, keys []Key) ([]Value, error) {
	vals := make([]Value, len(keys))
	var err error
	for i, k := range keys {
		vals[i], err = tx.Get(ctx, k)
		if err == ErrNotFound {
			vals[i] = nil
		} else if err != nil {
			return nil, err
		}
	}
	return vals, nil
}
