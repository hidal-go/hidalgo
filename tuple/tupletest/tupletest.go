package tupletest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hidal-go/hidalgo/filter"
	hkv "github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/kvtest"
	"github.com/hidal-go/hidalgo/tuple"
	tuplekv "github.com/hidal-go/hidalgo/tuple/kv"
	"github.com/hidal-go/hidalgo/values"
)

// Func is a constructor for database implementations.
// It returns an empty database and a function to destroy it.
type Func func(t testing.TB) tuple.Store

type Options struct {
	NoLocks bool // not safe for concurrent writes
}

// RunTest runs all tests for tuple store implementations.
func RunTest(t *testing.T, fnc Func, opts *Options) {
	if opts == nil {
		opts = &Options{}
	}
	for _, c := range testList {
		t.Run(c.name, func(t *testing.T) {
			db := fnc(t)
			c.test(t, db)
		})
	}
	t.Run("kv", func(t *testing.T) {
		kvtest.RunTest(t, func(t testing.TB) hkv.KV {
			db := fnc(t)

			ctx := context.TODO()
			kdb, err := tuplekv.NewKV(ctx, db, "kv")
			if err != nil {
				require.NoError(t, err)
			}
			t.Cleanup(func() {
				_ = kdb.Close()
			})
			return flat.Upgrade(kdb)
		}, &kvtest.Options{
			NoLocks: opts.NoLocks,
			NoTx:    true, // FIXME
		})
	})
}

var testList = []struct {
	name string
	test func(t *testing.T, db tuple.Store)
}{
	{name: "basic", test: basic},
	{name: "typed", test: typed},
	{name: "scans", test: scans},
	{name: "tables", test: tables},
	{name: "auto", test: auto},
}

func basic(t *testing.T, db tuple.Store) {
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

	it := tbl.Scan(&tuple.ScanOptions{Sort: tuple.SortAsc})
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

func typed(t *testing.T, db tuple.Store) {
	tx, err := db.Tx(true)
	require.NoError(t, err)
	defer tx.Close()

	sortable := []values.Sortable{
		values.String("foo"),
		values.Bytes("b\x00r"),
		values.Int(-42),
		values.UInt(42),
		values.Bool(false),
		// FIXME: test nanoseconds on backends that support it
		values.AsTime(time.Unix(123, 456789000)),
	}

	var payloads []values.Value
	for _, tp := range sortable {
		payloads = append(payloads, tp)
	}

	var (
		kfields  []tuple.KeyField
		kfields1 []tuple.KeyField
		vfields  []tuple.Field

		key  tuple.Key
		key1 tuple.Key
		data tuple.Data
	)

	for i, v := range sortable {
		kf := tuple.KeyField{
			Name: fmt.Sprintf("k%d", i+1),
			Type: v.SortableType(),
		}
		if i == 0 {
			key1 = append(key1, v)
			kfields1 = append(kfields1, kf)
		}
		key = append(key, v)
		kfields = append(kfields, kf)
	}
	for i, v := range payloads {
		data = append(data, v)
		vfields = append(vfields, tuple.Field{
			Name: fmt.Sprintf("p%d", i+1),
			Type: v.Type(),
		})
	}

	ctx := context.TODO()
	tbl1, err := tx.CreateTable(ctx, tuple.Header{
		Name: "test1", Key: kfields1, Data: vfields,
	})
	require.NoError(t, err, "\nkey : %#v\ndata: %#v", kfields1, vfields)
	tbl, err := tx.CreateTable(ctx, tuple.Header{
		Name: "test2", Key: kfields, Data: vfields,
	})
	require.NoError(t, err, "\nkey : %#v\ndata: %#v", kfields, vfields)

	_, err = tbl1.InsertTuple(ctx, tuple.Tuple{
		Key: key1, Data: data,
	})
	require.NoError(t, err)

	v2, err := tbl1.GetTuple(ctx, key1)
	require.NoError(t, err, "\nkey : %#v\ndata: %#v", kfields1, vfields)
	require.Equal(t, data, v2, "\n%v\nvs\n%v", data, v2)

	_, err = tbl.InsertTuple(ctx, tuple.Tuple{
		Key: key, Data: data,
	})
	require.NoError(t, err)

	v2, err = tbl.GetTuple(ctx, key)
	require.NoError(t, err, "\nkey : %#v\ndata: %#v", kfields, vfields)
	require.Equal(t, data, v2, "\n%v\nvs\n%v", data, v2)

	it := tbl.Scan(&tuple.ScanOptions{Sort: tuple.SortAsc})
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

func scans(t *testing.T, db tuple.Store) {
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
		it := tbl.Scan(&tuple.ScanOptions{Sort: tuple.SortAsc, Filter: f})
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

	tbl2, err := tx.CreateTable(ctx, tuple.Header{
		Name: "test2",
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

	_, err = tbl2.InsertTuple(ctx, tuple.Tuple{
		Key: tuple.SKey("a", "b", "a"), Data: tuple.Data{values.Int(-1)},
	})
	require.NoError(t, err)

	scan(nil, 1, 5, 3, 6, 4, 2)
}

func tables(t *testing.T, db tuple.Store) {
	t.Run("simple", func(t *testing.T) {
		tablesSimple(t, db)
	})
	t.Run("auto", func(t *testing.T) {
		tablesAuto(t, db)
	})
}

func tablesSimple(t testing.TB, db tuple.Store) {
	ctx := context.Background()
	const name = "test1"

	schema := tuple.Header{
		Name: name,
		Key: []tuple.KeyField{
			{Name: "key1", Type: values.StringType{}},
		},
		Data: []tuple.Field{
			{Name: "val1", Type: values.StringType{}},
		},
	}

	newTx := func(rw bool) tuple.Tx {
		tx, err := db.Tx(rw)
		require.NoError(t, err)
		return tx
	}

	tx := newTx(false)

	notExists := func() {
		tbl, err := tx.Table(ctx, name)
		require.Equal(t, tuple.ErrTableNotFound, err)
		require.Nil(t, tbl)
	}

	// access table when it not exists
	notExists()

	list, err := tx.ListTables(ctx)
	require.NoError(t, err)
	require.Empty(t, list)

	// create table on read-only transaction
	_, err = tx.CreateTable(ctx, schema)
	require.Equal(t, tuple.ErrReadOnly, err)

	notExists()

	err = tx.Close()
	require.NoError(t, err)

	// reopen read-write transaction
	tx = newTx(true)

	// table should not exist after failed creation
	tbl, err := tx.Table(ctx, name)
	require.Equal(t, tuple.ErrTableNotFound, err)
	require.Nil(t, tbl)

	tbl, err = tx.CreateTable(ctx, schema)
	require.NoError(t, err)
	require.NotNil(t, tbl)

	// TODO: check create + rollback
	err = tx.Commit(ctx)
	require.NoError(t, err)

	tx = newTx(true)

	tbl, err = tx.Table(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, schema, tbl.Header())

	err = tbl.Drop(ctx)
	require.NoError(t, err)

	tbl, err = tx.Table(ctx, name)
	require.Equal(t, tuple.ErrTableNotFound, err)
	require.Nil(t, tbl)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	tx = newTx(false)

	notExists()

	err = tx.Close()
	require.NoError(t, err)

	// TODO: test multiple tables
	// TODO: test different headers (only keys, only values)
}

func tablesAuto(t testing.TB, db tuple.Store) {
	ctx := context.Background()
	const name = "test2"

	schema := tuple.Header{
		Name: name,
		Key: []tuple.KeyField{
			{Name: "key1", Type: values.UIntType{}, Auto: true},
		},
		Data: []tuple.Field{
			{Name: "val1", Type: values.StringType{}},
		},
	}

	newTx := func(rw bool) tuple.Tx {
		tx, err := db.Tx(rw)
		require.NoError(t, err)
		return tx
	}

	tx := newTx(true)

	tbl, err := tx.CreateTable(ctx, schema)
	require.NoError(t, err)
	require.NotNil(t, tbl)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	tx = newTx(false)

	tbl, err = tx.Table(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, schema, tbl.Header())

	err = tx.Close()
	require.NoError(t, err)
}

func auto(t *testing.T, db tuple.Store) {
	tx, err := db.Tx(true)
	require.NoError(t, err)
	defer tx.Close()

	ctx := context.TODO()
	tbl, err := tx.CreateTable(ctx, tuple.Header{
		Name: "test",
		Key: []tuple.KeyField{
			{Name: "k1", Type: values.UIntType{}, Auto: true},
		},
		Data: []tuple.Field{
			{Name: "f1", Type: values.StringType{}},
		},
	})
	require.NoError(t, err)

	v1 := tuple.SData("1")
	k1, err := tbl.InsertTuple(ctx, tuple.Tuple{
		Key: tuple.AutoKey(), Data: v1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(k1))
	require.NotNil(t, k1[0])

	v2, err := tbl.GetTuple(ctx, k1)
	require.NoError(t, err)
	require.Equal(t, v1, v2)

	it := tbl.Scan(&tuple.ScanOptions{Sort: tuple.SortAsc})
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
