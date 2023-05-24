// Package kv provides an abstraction over hierarchical key-value stores.
package kv

import (
	"bytes"
	"context"
	"errors"

	"github.com/hidal-go/hidalgo/base"
)

var (
	// ErrNotFound is returned then a key was not found in the database.
	ErrNotFound = errors.New("kv: not found")
	// ErrReadOnly is returned when write operation is performed on read-only database or transaction.
	ErrReadOnly = errors.New("kv: read only")
	// ErrConflict is returned when write operation performed be current transaction cannot be committed
	// because of another concurrent write. Caller must restart the transaction.
	ErrConflict = errors.New("kv: read only")
)

// KV is an interface for hierarchical key-value databases.
type KV interface {
	base.DB
	Tx(ctx context.Context, rw bool) (Tx, error)
	View(ctx context.Context, fn func(tx Tx) error) error
	Update(ctx context.Context, fn func(tx Tx) error) error
}

// Key is a hierarchical binary key used in a database.
type Key [][]byte

// SKey is a helper for making string keys.
func SKey(parts ...string) Key {
	k := make(Key, 0, len(parts))
	for _, s := range parts {
		k = append(k, []byte(s))
	}
	return k
}

// Compare return 0 when keys are equal, -1 when k < k2 and +1 when k > k2.
func (k Key) Compare(k2 Key) int {
	for i, s := range k {
		if i >= len(k2) {
			return +1
		}
		if d := bytes.Compare(s, k2[i]); d != 0 {
			return d
		}
	}
	if len(k) < len(k2) {
		return -1
	}
	return 0
}

// HasPrefix checks if a key has a given prefix.
func (k Key) HasPrefix(pref Key) bool {
	if len(pref) == 0 {
		return true
	} else if len(k) < len(pref) {
		return false
	}
	for i, p := range pref {
		s := k[i]
		if i == len(pref)-1 {
			if !bytes.HasPrefix(s, p) {
				return false
			}
		} else {
			if !bytes.Equal(s, p) {
				return false
			}
		}
	}
	return true
}

// Append key parts and return a new value.
// Value is not a deep copy, use Clone for this.
func (k Key) Append(parts Key) Key {
	if k == nil && parts == nil {
		return nil
	}
	k2 := make(Key, len(k)+len(parts))
	i := copy(k2, k)
	copy(k2[i:], parts)
	return k2
}

// AppendBytes is the same like Append, but accepts bytes slices.
func (k Key) AppendBytes(parts ...[]byte) Key {
	return k.Append(Key(parts))
}

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

// ByKey sorts keys in ascending order.
type ByKey []Key

func (s ByKey) Len() int {
	return len(s)
}

func (s ByKey) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}

func (s ByKey) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
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

type Getter interface {
	// Get fetches a value for a single key from the database.
	// It return ErrNotFound if key does not exists.
	Get(ctx context.Context, key Key) (Value, error)
}

// Tx is a transaction over hierarchical key-value store.
type Tx interface {
	base.Tx
	Getter
	// GetBatch fetches values for multiple keys from the database.
	// Nil element in the slice indicates that key does not exists.
	GetBatch(ctx context.Context, keys []Key) ([]Value, error)
	// Put writes a key-value pair to the database.
	// New value will immediately be visible by Get on the same Tx,
	// but implementation might buffer the write until transaction is committed.
	Put(ctx context.Context, k Key, v Value) error
	// Del removes the key from the database. See Put for consistency guaranties.
	Del(ctx context.Context, k Key) error
	// Scan starts iteration over key-value pairs. Returned results are affected by IteratorOption.
	Scan(ctx context.Context, opts ...IteratorOption) Iterator
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
