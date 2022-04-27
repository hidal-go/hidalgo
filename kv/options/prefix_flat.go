package options

import (
	"bytes"
	"context"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
)

// WithPrefixFlat returns IteratorOption that limits scanned key to a given binary prefix.
// Store implementations can optimize this by implementing flat.PrefixIterator.
func WithPrefixFlat(pref flat.Key) IteratorOption {
	return PrefixFlat{Pref: pref}
}

// PrefixFlat implements IteratorOption. See WithPrefixFlat.
type PrefixFlat struct {
	Pref flat.Key
}

func (opt PrefixFlat) ApplyFlat(it flat.Iterator) flat.Iterator {
	if it, ok := it.(flat.PrefixIterator); ok {
		return it.WithPrefix(opt.Pref)
	}
	return &prefixIteratorFlat{base: it, pref: opt.Pref}
}

func (opt PrefixFlat) ApplyKV(it kv.Iterator) kv.Iterator {
	pref := flat.KeyUnescape(opt.Pref)
	if it, ok := it.(kv.PrefixIterator); ok {
		return it.WithPrefix(pref)
	}
	return &prefixIteratorKV{base: it, pref: pref}
}

var _ flat.PrefixIterator = &prefixIteratorFlat{}

type prefixIteratorFlat struct {
	base flat.Iterator
	pref flat.Key
	seek bool
	done bool
}

func (it *prefixIteratorFlat) reset() {
	it.seek = false
	it.done = false
}

func (it *prefixIteratorFlat) Reset() {
	it.base.Reset()
	it.reset()
}

func (it *prefixIteratorFlat) WithPrefix(pref flat.Key) flat.Iterator {
	if len(pref) == 0 {
		return it.base
	}
	it.pref = pref
	it.reset()
	return it
}

func (it *prefixIteratorFlat) Next(ctx context.Context) bool {
	if it.done {
		return false
	}

	if !it.seek {
		found := flat.Seek(ctx, it.base, it.pref)
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
	if bytes.HasPrefix(key, it.pref) {
		return true
	}

	// keys are sorted, and we reached the end of the prefix
	it.done = true
	return false
}

func (it *prefixIteratorFlat) Err() error {
	return it.base.Err()
}

func (it *prefixIteratorFlat) Close() error {
	return it.base.Close()
}

func (it *prefixIteratorFlat) Key() flat.Key {
	return it.base.Key()
}

func (it *prefixIteratorFlat) Val() flat.Value {
	return it.base.Val()
}
