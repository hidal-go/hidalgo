package kvtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hidal-go/hidalgo/kv"
)

func NewTest(t testing.TB, db kv.KV) *Test {
	return &Test{t: t, db: db}
}

type Test struct {
	t  testing.TB
	db kv.KV
}

func (t Test) Get(key kv.Key) (kv.Value, error) {
	tx, err := t.db.Tx(false)
	require.NoError(t.t, err)
	defer tx.Close()
	return tx.Get(context.TODO(), key)
}

func (t Test) NotExists(k kv.Key) {
	v, err := t.Get(k)
	require.Equal(t.t, kv.ErrNotFound, err)
	require.Equal(t.t, kv.Value(nil), v)
}

func (t Test) Expect(k kv.Key, exp kv.Value) {
	v, err := t.Get(k)
	require.NoError(t.t, err)
	require.Equal(t.t, exp, v)
}

func (t Test) Put(key kv.Key, val kv.Value) {
	tx, err := t.db.Tx(true)
	require.NoError(t.t, err)
	defer tx.Close()
	err = tx.Put(key, val)
	require.NoError(t.t, err)

	ctx := context.TODO()
	got, err := tx.Get(ctx, key)
	require.NoError(t.t, err)
	require.Equal(t.t, val, got)
	err = tx.Commit(ctx)
	require.NoError(t.t, err)
}

func (t Test) Del(key kv.Key) {
	tx, err := t.db.Tx(true)
	require.NoError(t.t, err)
	defer tx.Close()

	err = tx.Del(key)
	require.NoError(t.t, err)

	ctx := context.TODO()
	got, err := tx.Get(ctx, key)
	require.Equal(t.t, kv.ErrNotFound, err)
	require.Equal(t.t, kv.Value(nil), got)

	err = tx.Commit(ctx)
	require.NoError(t.t, err)
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

	require.NoError(t.t, it.Err())
	require.Equal(t.t, exp, got)
}

func (t Test) Scan(exp []kv.Pair, opts ...kv.IteratorOption) {
	tx, err := t.db.Tx(false)
	require.NoError(t.t, err)
	defer tx.Close()

	it := tx.Scan(opts...)
	defer it.Close()
	require.NoError(t.t, it.Err())

	t.ExpectIt(it, exp)
}

func (t Test) ScanReset(exp []kv.Pair, opts ...kv.IteratorOption) {
	tx, err := t.db.Tx(false)
	require.NoError(t.t, err)
	defer tx.Close()

	it := tx.Scan(opts...)
	defer it.Close()
	require.NoError(t.t, it.Err())

	t.ExpectIt(it, exp)
	it.Reset()
	t.ExpectIt(it, exp)
}
