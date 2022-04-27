package flat

import (
	"bytes"
	"context"

	"github.com/hidal-go/hidalgo/base"
)

// Iterator is an iterator over hierarchical key-value store.
type Iterator interface {
	base.Iterator
	// Reset the iterator to the starting state. Closed iterator can not reset.
	Reset()
	// Key return current key. Returned value will become invalid on Next or Close.
	// Caller should not modify or store the value - use Clone.
	Key() Key
	// Val return current value. Returned value will become invalid on Next or Close.
	// Caller should not modify or store the value - use Clone.
	Val() Value
}

type Seeker interface {
	Iterator
	// Seek the iterator to a given key. If the key does not exist, the next key is used.
	// Function returns false if there is no key greater or equal to a given one.
	Seek(ctx context.Context, key Key) bool
}

// Seek the iterator to a given key. If the key does not exist, the next key is used.
// Function returns false if there is no key greater or equal to a given one.
func Seek(ctx context.Context, it Iterator, key Key) bool {
	if it, ok := it.(Seeker); ok {
		return it.Seek(ctx, key)
	}

	if len(key) == 0 {
		it.Reset() // seek to the beginning
		return it.Next(ctx)
	}

	// check where we currently are
	k := it.Key()
	if len(k) == 0 {
		// might either be the beginning, or the end, so restart to be sure
		it.Reset()
	} else {
		switch bytes.Compare(k, key) {
		case 0:
			return true // already there
		case -1:
			// can seek forward without resetting
		case +1:
			it.Reset() // too far, must restart
		}
	}

	for it.Next(ctx) {
		k = it.Key()
		if bytes.Compare(k, key) >= 0 {
			return true
		}
	}

	return false
}

// ApplyIteratorOptions applies all iterator options.
func ApplyIteratorOptions(it Iterator, opts []IteratorOption) Iterator {
	for _, opt := range opts {
		it = opt.ApplyFlat(it)
	}
	return it
}

// IteratorOption is an additional option that affects iterator behaviour.
//
// Implementations in generic KV package should assert for an optimized version of option (via interface assertion),
// and fallback to generic implementation if the store doesn't support this option natively.
type IteratorOption interface {
	// ApplyFlat option to the flat KV iterator. Implementation may wrap or replace the iterator.
	ApplyFlat(it Iterator) Iterator
}

// IteratorOptionFunc is a function type that implements IteratorOption.
type IteratorOptionFunc func(it Iterator) Iterator

func (opt IteratorOptionFunc) ApplyFlat(it Iterator) Iterator {
	return opt(it)
}

// PrefixIterator is an Iterator optimization to support WithPrefix option.
type PrefixIterator interface {
	Iterator
	// WithPrefix implements WithPrefix iterator option.
	// Current iterator will be replaced with a new one and must not be used after this call.
	WithPrefix(pref Key) Iterator
}
