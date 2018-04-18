package tupletest

import (
	"context"
	"testing"

	hkv "github.com/nwca/hidalgo/kv"
	"github.com/nwca/hidalgo/kv/flat"
	"github.com/nwca/hidalgo/kv/kvtest"
	"github.com/nwca/hidalgo/tuple"
	"github.com/nwca/hidalgo/tuple/kv"
	"github.com/nwca/hidalgo/types"
	"github.com/stretchr/testify/require"
)

// Func is a constructor for database implementations.
// It returns an empty database and a function to destroy it.
type Func func(t testing.TB) (tuple.Store, func())

// RunTest runs all tests for tuple store implementations.
func RunTest(t *testing.T, fnc Func) {
	for _, c := range testList {
		t.Run(c.name, func(t *testing.T) {
			db, closer := fnc(t)
			defer closer()
			c.test(t, db)
		})
	}
	t.Run("kv", func(t *testing.T) {
		kvtest.RunTest(t, func(t testing.TB) (hkv.KV, func()) {
			db, closer := fnc(t)

			ctx := context.TODO()
			kdb, err := kv.NewKV(ctx, db, "kv")
			if err != nil {
				closer()
				require.NoError(t, err)
			}
			return flat.Upgrade(kdb), func() {
				kdb.Close()
				closer()
			}
		})
	})
}

var testList = []struct {
	name string
	test func(t testing.TB, db tuple.Store)
}{
	{name: "basic", test: basic},
}

func basic(t testing.TB, db tuple.Store) {
	tx, err := db.Tx(true)
	require.NoError(t, err)
	defer tx.Close()

	ctx := context.TODO()
	tbl, err := tx.CreateTable(ctx, tuple.Header{
		Name: "test",
		Key: []tuple.KeyField{
			{Name: "k1", Type: types.StringType{}},
		},
		Data: []tuple.Field{
			{Name: "f1", Type: types.StringType{}},
		},
	})
	require.NoError(t, err)

	k1 := types.String("a")
	v1 := types.String("1")
	_, err = tbl.InsertTuple(ctx, tuple.Tuple{
		Key:  tuple.Key{&k1},
		Data: tuple.Data{&v1},
	})
	require.NoError(t, err)

	v2, err := tbl.GetTuple(ctx, tuple.Key{&k1})
	require.NoError(t, err)
	require.Equal(t, tuple.Data{&v1}, v2)

	it := tbl.Scan(nil)
	defer it.Close()

	var tuples []tuple.Tuple
	for it.Next(ctx) {
		tuples = append(tuples, tuple.Tuple{
			Key: it.Key(), Data: it.Data(),
		})
	}
	require.NoError(t, it.Err())
	require.Equal(t, []tuple.Tuple{
		{Key: tuple.Key{&k1}, Data: tuple.Data{&v1}},
	}, tuples)
}
