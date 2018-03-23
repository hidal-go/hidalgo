package kvtest

import (
	"context"
	"sort"
	"testing"

	"github.com/nwca/hidalgo/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func (t Test) Scan(pref kv.Key, exp []kv.Pair) {
	tx, err := t.db.Tx(false)
	require.NoError(t.t, err)
	defer tx.Close()

	it := tx.Scan(pref)
	defer it.Close()
	require.NoError(t.t, it.Err())

	ctx := context.TODO()
	var got []kv.Pair
	for it.Next(ctx) {
		got = append(got, kv.Pair{
			Key: it.Key().Clone(),
			Val: it.Val().Clone(),
		})
	}
	require.NoError(t.t, it.Err())
	if assert.ObjectsAreEqual(exp, got) {
		return // ok
	}
	// check sorting order
	sort.Slice(got, func(i, j int) bool {
		return got[i].Key.Compare(got[j].Key) < 0
	})
	if assert.ObjectsAreEqual(exp, got) {
		// fail in any case
		assert.Fail(t.t, "results are not sorted")
		return
	}
	require.Equal(t.t, exp, got, "\n%v\nvs\n%v", exp, got)
}
