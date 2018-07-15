package tupletest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hidal-go/hidalgo/filter"
	hkv "github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/kvtest"
	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/tuple/kv"
	"github.com/hidal-go/hidalgo/values"
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
			kdb, err := tuplekv.NewKV(ctx, db, "kv")
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
	{name: "typed", test: typed},
	{name: "scans", test: scans},
}

func basic(t testing.TB, db tuple.Store) {
	tx, err := db.Tx(true)
	require.NoError(t, err)
	defer tx.Close()

	ctx := context.TODO()
	tbl, err := tx.CreateTable(ctx, tuple.Header{
		Name: "test",
		Key: []tuple.KeyField{
			{Name: "k1", Type: values.StringType{}},
		},
		Data: []tuple.Field{
			{Name: "f1", Type: values.StringType{}},
		},
	})
	require.NoError(t, err)

	k1 := tuple.SKey("a")
	v1 := tuple.SData("1")
	_, err = tbl.InsertTuple(ctx, tuple.Tuple{
		Key: k1, Data: v1,
	})
	require.NoError(t, err)

	v2, err := tbl.GetTuple(ctx, k1)
	require.NoError(t, err)
	require.Equal(t, v1, v2)

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
		{Key: k1, Data: v1},
	}, tuples)
}

func typed(t testing.TB, db tuple.Store) {
	tx, err := db.Tx(true)
	require.NoError(t, err)
	defer tx.Close()

	sortable := []values.Sortable{
		values.String("foo"),
		values.Bytes("b\x00r"),
		values.Int(-42),
		values.UInt(42),
		values.Bool(false),
		values.AsTime(time.Unix(123, 456)),
	}
	var payloads []values.Value
	for _, tp := range sortable {
		payloads = append(payloads, tp)
	}

	var (
		kfields []tuple.KeyField
		vfields []tuple.Field

		key  tuple.Key
		data tuple.Data
	)

	for i, v := range sortable {
		key = append(key, v)
		kfields = append(kfields, tuple.KeyField{
			Name: fmt.Sprintf("k%d", i+1),
			Type: v.SortableType(),
		})
	}
	for i, v := range payloads {
		data = append(data, v)
		vfields = append(vfields, tuple.Field{
			Name: fmt.Sprintf("p%d", i+1),
			Type: v.Type(),
		})
	}

	ctx := context.TODO()
	tbl, err := tx.CreateTable(ctx, tuple.Header{
		Name: "test", Key: kfields, Data: vfields,
	})
	require.NoError(t, err)

	_, err = tbl.InsertTuple(ctx, tuple.Tuple{
		Key: key, Data: data,
	})
	require.NoError(t, err)

	v2, err := tbl.GetTuple(ctx, key)
	require.NoError(t, err)
	require.Equal(t, data, v2)

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
		{Key: key, Data: data},
	}, tuples)
}

func scans(t testing.TB, db tuple.Store) {
	tx, err := db.Tx(true)
	require.NoError(t, err)
	defer tx.Close()

	ctx := context.TODO()
	tbl, err := tx.CreateTable(ctx, tuple.Header{
		Name: "test",
		Key: []tuple.KeyField{
			{Name: "k1", Type: values.StringType{}},
			{Name: "k2", Type: values.StringType{}},
			{Name: "k3", Type: values.StringType{}},
		},
		Data: []tuple.Field{
			{Name: "f1", Type: values.IntType{}},
		},
	})
	require.NoError(t, err)

	insert := func(key []string, n int) {
		var tkey tuple.Key
		for _, k := range key {
			tkey = append(tkey, values.String(k))
		}
		_, err = tbl.InsertTuple(ctx, tuple.Tuple{
			Key: tkey, Data: tuple.Data{values.Int(n)},
		})
		require.NoError(t, err)
	}

	scan := func(pref []string, exp ...int) {
		var kpref tuple.KeyFilters
		if len(pref) != 0 {
			for i, k := range pref {
				var f filter.SortableFilter
				if i == len(pref)-1 {
					if k == "" {
						break
					}
					f = filter.Prefix(values.String(k))
				} else {
					f = filter.EQ(values.String(k))
				}
				kpref = append(kpref, f)
			}
		}
		var f *tuple.Filter
		if kpref != nil {
			f = &tuple.Filter{KeyFilter: kpref}
		}
		it := tbl.Scan(f)
		defer it.Close()

		var got []int
		for it.Next(ctx) {
			d := it.Data()
			require.True(t, len(d) == 1)
			v, ok := d[0].(values.Int)
			require.True(t, ok, "%T: %#v", d[0], d[0])
			got = append(got, int(v))
		}
		require.Equal(t, exp, got)
	}

	insert([]string{"a", "a", "a"}, 1)
	insert([]string{"b", "b", "b"}, 2)
	insert([]string{"a", "aa", "b"}, 3)
	insert([]string{"a", "ba", "c"}, 4)
	insert([]string{"a", "a", "ab"}, 5)
	insert([]string{"a", "b", "c"}, 6)

	scan(nil, 1, 5, 3, 6, 4, 2)
	scan([]string{""}, 1, 5, 3, 6, 4, 2)
	scan([]string{"a"}, 1, 5, 3, 6, 4)
	scan([]string{"b"}, 2)
	scan([]string{"a", "a"}, 1, 5, 3)
	scan([]string{"a", "a", ""}, 1, 5)
	scan([]string{"a", "aa"}, 3)
	scan([]string{"a", "aa", ""}, 3)
	scan([]string{"a", "aa", "b"}, 3)
}
