package flat

import (
	"context"

	"github.com/hidal-go/hidalgo/kv"
)

var _ kv.KV = (*hieKV)(nil)

const (
	sep = '/'
	esc = '\\'
)

// Upgrade upgrades flat KV to hierarchical KV.
func Upgrade(flat KV) kv.KV {
	return &hieKV{flat: flat}
}

// UpgradeOpenPath automatically upgrades flat KV to hierarchical KV on open.
func UpgradeOpenPath(open OpenPathFunc) kv.OpenPathFunc {
	return func(path string) (kv.KV, error) {
		flat, err := open(path)
		if err != nil {
			return nil, err
		}

		return Upgrade(flat), nil
	}
}

type hieKV struct {
	flat KV
}

// KeyEscape converts kv.Key to a flat Key.
func KeyEscape(k kv.Key) Key {
	var k2 Key
	for i, s := range k {
		if i != 0 {
			k2 = append(k2, sep)
		}

		for _, p := range s {
			if p == esc || p == sep {
				k2 = append(k2, esc)
			}
			k2 = append(k2, p)
		}
	}

	return k2
}

// KeyUnescape converts flat Key into kv.Key.
func KeyUnescape(k Key) kv.Key {
	var (
		k2  kv.Key
		cur Key
	)

	for i := 0; i < len(k); i++ {
		p := k[i]
		if p == esc {
			cur = append(cur, k[i+1])
			i++

			continue
		} else if p == sep {
			k2 = append(k2, cur)
			cur = nil

			continue
		}
		cur = append(cur, p)
	}

	if cur != nil {
		k2 = append(k2, cur)
	}

	return k2
}

func (hkv *hieKV) Close() error {
	return hkv.flat.Close()
}

func (hkv *hieKV) Tx(rw bool) (kv.Tx, error) {
	tx, err := hkv.flat.Tx(rw)
	if err != nil {
		return nil, err
	}

	return &flatTx{kv: hkv, tx: tx, rw: rw}, nil
}

func (hkv *hieKV) View(ctx context.Context, fn func(tx kv.Tx) error) error {
	return kv.View(ctx, hkv, fn)
}

func (hkv *hieKV) Update(ctx context.Context, fn func(tx kv.Tx) error) error {
	return kv.Update(ctx, hkv, fn)
}

type flatTx struct {
	kv *hieKV
	tx Tx
	rw bool
}

func (tx *flatTx) key(key kv.Key) Key {
	if len(key) == 0 {
		return nil
	}

	return KeyEscape(key)
}

func (tx *flatTx) Get(ctx context.Context, key kv.Key) (kv.Value, error) {
	return tx.tx.Get(ctx, tx.key(key))
}

func (tx *flatTx) GetBatch(ctx context.Context, keys []kv.Key) ([]kv.Value, error) {
	ks := make([]Key, len(keys))
	for i, k := range keys {
		ks[i] = tx.key(k)
	}

	return tx.tx.GetBatch(ctx, ks)
}

func (tx *flatTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

func (tx *flatTx) Close() error {
	return tx.tx.Close()
}

func (tx *flatTx) Put(k kv.Key, v kv.Value) error {
	if !tx.rw {
		return kv.ErrReadOnly
	}

	return tx.tx.Put(tx.key(k), v)
}

func (tx *flatTx) Del(k kv.Key) error {
	if !tx.rw {
		return kv.ErrReadOnly
	}

	return tx.tx.Del(tx.key(k))
}

func (tx *flatTx) Scan(opts ...kv.IteratorOption) kv.Iterator {
	var (
		native   []IteratorOption
		fallback []kv.IteratorOption
	)

	for _, opt := range opts {
		if v, ok := opt.(IteratorOption); ok {
			native = append(native, v)
		} else {
			fallback = append(fallback, opt)
		}
	}

	it := &prefIter{kv: tx.kv, Iterator: tx.tx.Scan(native...)}

	return kv.ApplyIteratorOptions(it, fallback)
}

type prefIter struct {
	kv *hieKV
	Iterator
}

func (it *prefIter) Val() kv.Value {
	return it.Iterator.Val()
}

func (it *prefIter) Key() kv.Key {
	return KeyUnescape(it.Iterator.Key())
}
