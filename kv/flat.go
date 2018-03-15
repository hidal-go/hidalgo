package kv

import (
	"bytes"
	"context"
	"fmt"
)

type Flat interface {
	Base
	Tx(update bool) (FlatTx, error)
}

type FlatTx interface {
	Tx
	Bucket
}

var _ DB = (*flatKV)(nil)

func NewFlat(flat Flat) DB {
	return &flatKV{flat: flat}
}

type flatKV struct {
	flat Flat
}

func (kv *flatKV) Type() string { return kv.flat.Type() }
func (kv *flatKV) Close() error { return kv.flat.Close() }
func (kv *flatKV) Tx(update bool) (BucketTx, error) {
	tx, err := kv.flat.Tx(update)
	if err != nil {
		return nil, err
	}
	return &flatTx{kv: kv.flat, tx: tx, ro: !update}, nil
}

type flatTx struct {
	kv Flat
	tx FlatTx
	ro bool

	buckets map[string]*flatBucket
}

func (v *flatTx) Get(ctx context.Context, keys []BucketKey) ([][]byte, error) {
	ks := make([][]byte, len(keys))
	for i, k := range keys {
		ks[i] = v.bucketKey(k.Bucket, k.Key)
	}
	return v.tx.Get(ctx, ks)
}

func (v *flatTx) Commit(ctx context.Context) error {
	return v.tx.Commit(ctx)
}
func (v *flatTx) Rollback() error {
	return v.tx.Rollback()
}

const bucketSep = '/'

func (v *flatTx) bucketKey(name, key []byte) []byte {
	p := make([]byte, len(name)+1+len(key))
	n := copy(p, name)
	p[n] = bucketSep
	n++
	copy(p[n:], key)
	return p
}
func (v *flatTx) Bucket(name []byte) Bucket {
	if b := v.buckets[string(name)]; b != nil {
		return b
	}
	if v.buckets == nil {
		v.buckets = make(map[string]*flatBucket)
	}
	pref := v.bucketKey(name, nil)
	b := &flatBucket{flatTx: v, pref: pref}
	v.buckets[string(name)] = b
	return b
}

type flatBucket struct {
	*flatTx
	pref []byte
}

func (b *flatBucket) key(k []byte) []byte {
	key := make([]byte, len(b.pref)+len(k))
	n := copy(key, b.pref)
	copy(key[n:], k)
	return key
}
func (b *flatBucket) Get(ctx context.Context, keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	} else if len(keys) == 1 {
		return b.tx.Get(ctx, [][]byte{b.key(keys[0])})
	}
	nk := make([][]byte, len(keys))
	for i, k := range keys {
		nk[i] = b.key(k)
	}
	return b.tx.Get(ctx, nk)
}
func (b *flatBucket) Put(k, v []byte) error {
	if b.ro {
		return fmt.Errorf("put in ro tx")
	}
	return b.tx.Put(b.key(k), v)
}
func (b *flatBucket) Del(k []byte) error {
	if b.ro {
		return fmt.Errorf("del in ro tx")
	}
	return b.tx.Del(b.key(k))
}

func (b *flatBucket) Scan(pref []byte) Iterator {
	pref = b.key(pref)
	return &prefIter{Iterator: b.tx.Scan(pref), trim: b.pref}
}

type prefIter struct {
	Iterator
	trim []byte
}

func (it *prefIter) Key() []byte {
	return bytes.TrimPrefix(it.Iterator.Key(), it.trim)
}
