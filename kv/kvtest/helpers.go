package kvtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hidal-go/hidalgo/kv"
)

func NewTest(tb testing.TB, db kv.KV) *Test {
	return &Test{tb: tb, db: db}
}

type Test struct {
	tb testing.TB
	db kv.KV
}

func (t Test) Get(key kv.Key) (kv.Value, error) {
	tx, err := t.db.Tx(false)
	require.NoError(t.tb, err)
	defer tx.Close()
	return tx.Get(context.TODO(), key)
}

func (t Test) NotExists(k kv.Key) {
	v, err := t.Get(k)
	require.Equal(t.tb, kv.ErrNotFound, err)
	require.Equal(t.tb, kv.Value(nil), v)
}

func (t Test) Expect(k kv.Key, exp kv.Value) {
	v, err := t.Get(k)
	require.NoError(t.tb, err)
	require.Equal(t.tb, exp, v)
}

func (t Test) Put(key kv.Key, val kv.Value) {
	tx, err := t.db.Tx(true)
	require.NoError(t.tb, err)
	defer tx.Close()
	err = tx.Put(key, val)
	require.NoError(t.tb, err)

	ctx := context.TODO()
	got, err := tx.Get(ctx, key)
	require.NoError(t.tb, err)
	require.Equal(t.tb, val, got)
	err = tx.Commit(ctx)
	require.NoError(t.tb, err)
}

func (t Test) Del(key kv.Key) {
	tx, err := t.db.Tx(true)
	require.NoError(t.tb, err)
	defer tx.Close()

	err = tx.Del(key)
	require.NoError(t.tb, err)

	ctx := context.TODO()
	got, err := tx.Get(ctx, key)
	require.Equal(t.tb, kv.ErrNotFound, err)
	require.Equal(t.tb, kv.Value(nil), got)

	err = tx.Commit(ctx)
	require.NoError(t.tb, err)
}

func (t Test) ExpectIt(it kv.Iterator, exp []kv.Pair) {
	if len(exp) == 0 {
		exp = nil
	}
	ctx := context.TODO()
	var got []kv.Pair
	for it.Next(ctx) {
		got = append(got, kv.Pair{
			Key: it.Key().Clone(),
			Val: it.Val().Clone(),
		})
	}
	require.NoError(t.tb, it.Err())
	require.Equal(t.tb, exp, got)
}

func (t Test) Scan(exp []kv.Pair, opts ...kv.IteratorOption) {
	tx, err := t.db.Tx(false)
	require.NoError(t.tb, err)
	defer tx.Close()

	it := tx.Scan(opts...)
	defer it.Close()
	require.NoError(t.tb, it.Err())

	t.ExpectIt(it, exp)
}

func (t Test) ScanReset(exp []kv.Pair, opts ...kv.IteratorOption) {
	tx, err := t.db.Tx(false)
	require.NoError(t.tb, err)
	defer tx.Close()

	it := tx.Scan(opts...)
	defer it.Close()
	require.NoError(t.tb, it.Err())

	t.ExpectIt(it, exp)
	it.Reset()
	t.ExpectIt(it, exp)
}
