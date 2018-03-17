package flat

import (
	"bytes"
	"context"

	"github.com/nwca/uda/kv"
)

var _ kv.KV = (*flatKV)(nil)

func New(flat KV, sep byte) kv.KV {
	return &flatKV{flat: flat, sep: sep}
}

type flatKV struct {
	flat KV
	sep  byte
}

func (kv *flatKV) Close() error {
	return kv.flat.Close()
}
func (kv *flatKV) Tx(rw bool) (kv.Tx, error) {
	tx, err := kv.flat.Tx(rw)
	if err != nil {
		return nil, err
	}
	return &flatTx{kv: kv, tx: tx, rw: rw}, nil
}

type flatTx struct {
	kv *flatKV
	tx Tx
	rw bool
}

func (tx *flatTx) key(key kv.Key) Key {
	l := len(key)
	if l == 0 {
		return nil
	} else if l == 1 {
		return key[0]
	}
	n := 0
	for _, k := range key {
		n += len(k)
	}
	p := make([]byte, n+l)
	i := 0
	sep := tx.kv.sep
	for _, k := range key {
		i += copy(p[i:], k)
		p[i] = sep
		i++
	}
	return p[:len(p)-1]
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

func (tx *flatTx) Scan(pref kv.Key) kv.Iterator {
	return &prefIter{kv: tx.kv, Iterator: tx.tx.Scan(tx.key(pref))}
}

type prefIter struct {
	kv *flatKV
	Iterator
}

func (it *prefIter) Val() kv.Value {
	return it.Iterator.Val()
}

func (it *prefIter) Key() kv.Key {
	return bytes.Split(it.Iterator.Key(), []byte{it.kv.sep})
}
