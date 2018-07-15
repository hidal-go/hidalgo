package kvdebug

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/hidal-go/hidalgo/kv"
)

var _ kv.KV = (*KV)(nil)

func New(kv kv.KV) *KV {
	return &KV{KV: kv}
}

type Stats struct {
	Errs int64
	Tx   struct {
		RO int64
		RW int64
	}
	Get struct {
		N     int64
		Batch int64
		Miss  int64
	}
	Put struct {
		N int64
	}
	Del struct {
		N int64
	}
	Iter struct {
		N    int64
		Next int64
		K, V int64
	}
}

type KV struct {
	stats   Stats
	running struct {
		txRO int64
		txRW int64
		iter int64
	}
	log bool

	KV kv.KV
}

func (d *KV) logging() bool {
	return d.log
}
func (d *KV) Log(v bool) {
	d.log = v
}
func (d *KV) Stats() Stats {
	return d.stats
}
func (d *KV) Close() error {
	err := d.KV.Close()
	if err != nil {
		atomic.AddInt64(&d.stats.Errs, 1)
	}
	r := atomic.LoadInt64(&d.running.txRO)
	w := atomic.LoadInt64(&d.running.txRW)
	s := atomic.LoadInt64(&d.running.iter)
	if r+w+s != 0 {
		panic(fmt.Errorf("resourse leak: iter: %d, ro: %d, rw: %d", s, r, w))
	}
	return err
}

func (d *KV) Tx(rw bool) (kv.Tx, error) {
	tx, err := d.KV.Tx(rw)
	if err != nil {
		if tx != nil {
			panic("tx should be nil on error")
		}
		atomic.AddInt64(&d.stats.Errs, 1)
		return nil, err
	}
	if rw {
		atomic.AddInt64(&d.stats.Tx.RW, 1)
		atomic.AddInt64(&d.running.txRW, 1)
	} else {
		atomic.AddInt64(&d.stats.Tx.RO, 1)
		atomic.AddInt64(&d.running.txRO, 1)
	}
	return &kvTX{kv: d, tx: tx, rw: rw}, nil
}

type kvTX struct {
	kv  *KV
	tx  kv.Tx
	err error
	rw  bool
}

func (tx *kvTX) done(err error) {
	tx.err = err
	tx.tx = nil

	d := tx.kv
	if err != nil {
		atomic.AddInt64(&d.stats.Errs, 1)
	}
	if tx.rw {
		atomic.AddInt64(&d.running.txRW, -1)
	} else {
		atomic.AddInt64(&d.running.txRO, -1)
	}
}
func (tx *kvTX) Commit(ctx context.Context) error {
	if tx.tx == nil {
		return tx.err
	}
	err := tx.tx.Commit(ctx)
	tx.done(err)
	return err
}

func (tx *kvTX) Close() error {
	if tx.tx == nil {
		return tx.err
	}
	err := tx.tx.Close()
	tx.done(err)
	return err
}

func (tx *kvTX) Get(ctx context.Context, k kv.Key) (kv.Value, error) {
	v, err := tx.tx.Get(ctx, k)
	d := tx.kv
	atomic.AddInt64(&d.stats.Get.N, 1)
	if err == kv.ErrNotFound {
		atomic.AddInt64(&d.stats.Get.Miss, 1)
	} else if err != nil {
		atomic.AddInt64(&d.stats.Errs, 1)
	}
	if d.logging() {
		log.Printf("get: %q = %q (%v)", k, v, err)
	}
	return v, err
}

func (tx *kvTX) GetBatch(ctx context.Context, keys []kv.Key) ([]kv.Value, error) {
	vals, err := tx.tx.GetBatch(ctx, keys)
	d := tx.kv
	atomic.AddInt64(&d.stats.Get.Batch, int64(len(keys)))
	if err != nil {
		atomic.AddInt64(&d.stats.Errs, 1)
	}
	for _, v := range vals {
		if v == nil {
			atomic.AddInt64(&d.stats.Get.Miss, 1)
		}
	}
	if d.logging() {
		log.Printf("get batch: %d (%v)", len(keys), err)
		for i := range vals {
			log.Printf("get: %q = %q", keys[i], vals[i])
		}
	}
	return vals, err
}

func (tx *kvTX) Put(k kv.Key, v kv.Value) error {
	if !tx.rw {
		panic("put in RO transaction")
	}
	err := tx.tx.Put(k, v)
	d := tx.kv
	atomic.AddInt64(&d.stats.Put.N, 1)
	if err != nil {
		atomic.AddInt64(&d.stats.Errs, 1)
	}
	if d.logging() {
		log.Printf("put: %q = %q (%v)", k, v, err)
	}
	return err
}

func (tx *kvTX) Del(k kv.Key) error {
	if !tx.rw {
		panic("del in RO transaction")
	}
	err := tx.tx.Del(k)
	d := tx.kv
	atomic.AddInt64(&d.stats.Del.N, 1)
	if err != nil {
		atomic.AddInt64(&d.stats.Errs, 1)
	}
	if d.logging() {
		log.Printf("del: %q (%v)", k, err)
	}
	return err
}

func (tx *kvTX) Scan(pref kv.Key) kv.Iterator {
	d := tx.kv
	atomic.AddInt64(&d.running.iter, 1)
	atomic.AddInt64(&d.stats.Iter.N, 1)
	if d.logging() {
		log.Printf("scan: %q", pref)
	}
	return &kvIter{kv: tx.kv, it: tx.tx.Scan(pref), pref: pref}
}

type kvIter struct {
	kv   *KV
	pref kv.Key
	it   kv.Iterator
	err  error
}

func (it *kvIter) Next(ctx context.Context) bool {
	d := it.kv
	if !it.it.Next(ctx) {
		if d.logging() {
			log.Printf("scan: %q: %v", it.pref, false)
		}
		return false
	}
	atomic.AddInt64(&d.stats.Iter.Next, 1)
	if d.logging() {
		log.Printf("scan: %q: %q = %q", it.pref, it.it.Key(), it.it.Val())
	}
	return true
}

func (it *kvIter) Err() error {
	return it.it.Err()
}

func (it *kvIter) Close() error {
	if it.it == nil {
		return it.err
	}
	err := it.it.Close()
	it.err = err
	it.it = nil

	d := it.kv
	if err != nil {
		atomic.AddInt64(&d.stats.Errs, 1)
	}
	atomic.AddInt64(&d.running.iter, -1)
	return err
}

func (it *kvIter) Key() kv.Key {
	d := it.kv
	atomic.AddInt64(&d.stats.Iter.K, 1)
	return it.it.Key()
}

func (it *kvIter) Val() kv.Value {
	d := it.kv
	atomic.AddInt64(&d.stats.Iter.V, 1)
	return it.it.Val()
}
