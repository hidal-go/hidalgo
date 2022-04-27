package options

import (
	"context"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
)

// WithPrefixKV returns IteratorOption that limits scanned key to a given binary prefix.
// Store implementations can optimize this by implementing kv.PrefixIterator.
func WithPrefixKV(pref kv.Key) IteratorOption {
	return PrefixKV{Pref: pref}
}

// PrefixKV implements IteratorOption. See WithPrefixKV.
type PrefixKV struct {
	Pref kv.Key
}

func (opt PrefixKV) ApplyKV(it kv.Iterator) kv.Iterator {
	if it, ok := it.(kv.PrefixIterator); ok {
		return it.WithPrefix(opt.Pref)
	}
	return &prefixIteratorKV{base: it, pref: opt.Pref}
}

func (opt PrefixKV) ApplyFlat(it flat.Iterator) flat.Iterator {
	pref := flat.KeyEscape(opt.Pref)
	if it, ok := it.(flat.PrefixIterator); ok {
		return it.WithPrefix(pref)
	}
	return &prefixIteratorFlat{base: it, pref: pref}
}

var _ kv.PrefixIterator = &prefixIteratorKV{}

type prefixIteratorKV struct {
	base kv.Iterator
	pref kv.Key
	seek bool
	done bool
}

func (it *prefixIteratorKV) reset() {
	it.seek = false
	it.done = false
}

func (it *prefixIteratorKV) Reset() {
	it.base.Reset()
	it.reset()
}

func (it *prefixIteratorKV) WithPrefix(pref kv.Key) kv.Iterator {
	if len(pref) == 0 {
		return it.base
	}
	it.pref = pref
	it.reset()
	return it
}

func (it *prefixIteratorKV) Next(ctx context.Context) bool {
	if it.done {
		return false
	}

	if !it.seek {
		found := kv.Seek(ctx, it.base, it.pref)
		it.seek = true
		if !found {
			it.done = true
			return false
		}
	} else {
		if !it.base.Next(ctx) {
			it.done = true
			return false
		}
	}

	key := it.base.Key()
	if key.HasPrefix(it.pref) {
		return true
	}

	// keys are sorted, and we reached the end of the prefix
	it.done = true
	return false
}

func (it *prefixIteratorKV) Err() error {
	return it.base.Err()
}

func (it *prefixIteratorKV) Close() error {
	return it.base.Close()
}

func (it *prefixIteratorKV) Key() kv.Key {
	return it.base.Key()
}

func (it *prefixIteratorKV) Val() kv.Value {
	return it.base.Val()
}
