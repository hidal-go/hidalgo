package options

import (
	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
)

var (
	_ kv.IteratorOption   = (IteratorOption)(nil)
	_ flat.IteratorOption = (IteratorOption)(nil)
)

// IteratorOption is an additional option that affects iterator behaviour.
//
// Implementations in generic KV package should assert for an optimized version of option (via interface assertion),
// and fallback to generic implementation if the store doesn't support this option natively.
type IteratorOption interface {
	// ApplyKV applies option to the KV iterator. Implementation may wrap or replace the iterator.
	ApplyKV(it kv.Iterator) kv.Iterator
	// ApplyFlat option to the flat KV iterator. Implementation may wrap or replace the iterator.
	ApplyFlat(it flat.Iterator) flat.Iterator
}
