package kvtest

import (
	"context"
	"testing"

	. "github.com/nwca/uda/kv"
	"github.com/stretchr/testify/require"
)

func NewTest(t testing.TB, db KV) *Test {
	return &Test{t: t, db: db}
}

type Test struct {
	t  testing.TB
	db KV
}

func (t Test) get(key Key) (Value, error) {
	tx, err := t.db.Tx(false)
	require.NoError(t.t, err)
	defer tx.Close()
	return tx.Get(context.TODO(), key)
}

func (t Test) notExists(k Key) {
	v, err := t.get(k)
	require.Equal(t.t, ErrNotFound, err)
	require.Equal(t.t, Value(nil), v)
}

func (t Test) expect(k Key, exp Value) {
	v, err := t.get(k)
	require.NoError(t.t, err)
	require.Equal(t.t, exp, v)
}

func (t Test) put(key Key, val Value) {
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

func (t Test) del(key Key) {
	tx, err := t.db.Tx(true)
	require.NoError(t.t, err)
	defer tx.Close()
	err = tx.Del(key)
	require.NoError(t.t, err)

	ctx := context.TODO()
	got, err := tx.Get(ctx, key)
	require.Equal(t.t, ErrNotFound, err)
	require.Equal(t.t, Value(nil), got)
	err = tx.Commit(ctx)
	require.NoError(t.t, err)
}

func (t Test) iterate(pref Key, exp []Pair) {
	tx, err := t.db.Tx(false)
	require.NoError(t.t, err)
	defer tx.Close()

	it := tx.Scan(pref)
	defer it.Close()
	require.NoError(t.t, it.Err())

	ctx := context.TODO()
	var got []Pair
	for it.Next(ctx) {
		got = append(got, Pair{
			Key: it.Key().Clone(),
			Val: it.Val().Clone(),
		})
	}
	require.NoError(t.t, it.Err())
	require.Equal(t.t, exp, got)
}
