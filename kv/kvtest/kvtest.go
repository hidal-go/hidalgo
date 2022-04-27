package kvtest

import (
	"context"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/options"
)

// Func is a constructor for database implementations.
// It returns an empty database and a function to destroy it.
type Func func(t testing.TB) kv.KV

type Options struct {
	NoLocks bool // not safe for concurrent writes
	NoTx    bool // implementation doesn't support proper transactions
}

// RunTest runs all tests for key-value implementations.
func RunTest(t *testing.T, fnc Func, opts *Options) {
	if opts == nil {
		opts = &Options{}
	}

	for _, c := range testList {
		t.Run(c.name, func(t *testing.T) {
			if c.concurrent && opts.NoLocks {
				t.Skip("implementation doesn't support concurrent writes")
			}

			if c.txOnly && opts.NoTx {
				t.Skip("implementation doesn't support transactions")
			}

			db := fnc(t)
			c.test(t, db)
		})
	}
}

// RunTestLocal is a wrapper for RunTest that automatically creates a temporary directory and opens a database.
func RunTestLocal(t *testing.T, open kv.OpenPathFunc, opts *Options) {
	if opts == nil {
		opts = &Options{}
	}

	RunTest(t, func(t testing.TB) kv.KV {
		dir, err := ioutil.TempDir("", "dal-kv-")
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = os.RemoveAll(dir)
		})

		db, err := open(dir)
		if err != nil {
			require.NoError(t, err)
		}

		t.Cleanup(func() {
			db.Close()
			db.Close() // test double close
		})

		return db
	}, opts)
}

var testList = []struct {
	name       string
	test       func(t testing.TB, db kv.KV)
	concurrent bool // requires concurrent safety
	txOnly     bool // requires transactions
}{
	{name: "basic", test: basic},
	{name: "ro", test: readonly},
	{name: "seek", test: seek},
	{name: "increment", test: increment, txOnly: true, concurrent: true},
}

func basic(t testing.TB, db kv.KV) {
	td := NewTest(t, db)

	keys := []kv.Key{
		{[]byte("a")},
		{[]byte("b"), []byte("a")},
		{[]byte("b"), []byte("a1")},
		{[]byte("b"), []byte("a2")},
		{[]byte("b"), []byte("b")},
		{[]byte("c")},
	}

	td.NotExists(nil)
	for _, k := range keys {
		td.NotExists(k)
	}

	var all []kv.Pair
	for i, k := range keys {
		v := kv.Value(strconv.Itoa(i))
		td.Put(k, v)
		td.Expect(k, v)
		all = append(all, kv.Pair{Key: k, Val: v})
	}

	td.ScanReset(all)
	td.ScanReset(all[:1], options.WithPrefixKV(keys[0]))
	td.ScanReset(all[len(all)-1:], options.WithPrefixKV(keys[len(keys)-1]))
	td.ScanReset(all[1:len(all)-1], options.WithPrefixKV(keys[1][:1]))
	td.ScanReset(all[1:4], options.WithPrefixKV(kv.Key{keys[1][0], keys[1][1][:1]}))

	for _, k := range keys {
		td.Del(k)
	}
	for _, k := range keys {
		td.NotExists(k)
	}
}

func readonly(t testing.TB, db kv.KV) {
	td := NewTest(t, db)

	key := kv.Key{[]byte("a")}
	val := []byte("v")
	td.Put(key, val)

	nokey := kv.Key{[]byte("b")}

	tx, err := db.Tx(false)
	require.NoError(t, err)
	defer tx.Close()

	// writing anything on read-only tx must fail
	err = tx.Put(key, val)
	require.Equal(t, kv.ErrReadOnly, err)
	err = tx.Put(nokey, val)
	require.Equal(t, kv.ErrReadOnly, err)

	// deleting records on read-only tx must fail
	err = tx.Del(key)
	require.Equal(t, kv.ErrReadOnly, err)

	// deleting non-existed record on read-only tx must still fail
	err = tx.Del(nokey)
	require.Equal(t, kv.ErrReadOnly, err)
}

func seek(t testing.TB, db kv.KV) {
	td := NewTest(t, db)

	keys := []kv.Key{
		{[]byte("a")},
		{[]byte("b"), []byte("a")},
		{[]byte("b"), []byte("a1")},
		{[]byte("b"), []byte("a2")},
		{[]byte("b"), []byte("b")},
		{[]byte("c")},
	}

	var all []kv.Pair
	for i, k := range keys {
		v := kv.Value(strconv.Itoa(i))
		td.Put(k, v)
		all = append(all, kv.Pair{Key: k, Val: v})
	}

	tx, err := db.Tx(false)
	require.NoError(t, err)
	defer tx.Close()

	ctx := context.TODO()

	// start an iterator and check if it can reset
	// this is the basic requirement for generic seek to work
	it := tx.Scan()
	defer it.Close()
	td.ExpectIt(it, all)
	it.Reset()
	td.ExpectIt(it, all)

	// seek to each key, current value must match corresponding element, and iterating further must return everything else
	for i, p := range all {
		ok := kv.Seek(ctx, it, p.Key)
		require.True(t, ok)
		require.Equal(t, p.Key, it.Key())
		require.Equal(t, p.Val, it.Val())
		td.ExpectIt(it, all[i+1:])
	}

	// seek to element, then seek to the next key (test offsets 1 and 2)
	for _, off := range []int{1, 2} {
		for i := range all[:len(all)-off] {
			ok := kv.Seek(ctx, it, all[i].Key)
			require.True(t, ok)
			require.Equal(t, all[i].Key, it.Key())
			require.Equal(t, all[i].Val, it.Val())

			ok = kv.Seek(ctx, it, all[i+off].Key)
			require.True(t, ok)
			require.Equal(t, all[i+off].Key, it.Key())
			require.Equal(t, all[i+off].Val, it.Val())

			td.ExpectIt(it, all[i+off+1:])
		}
	}

	// same, but seek backward instead
	for _, off := range []int{1, 2} {
		for i := range all {
			if i < off {
				continue
			}

			ok := kv.Seek(ctx, it, all[i].Key)
			require.True(t, ok)
			require.Equal(t, all[i].Key, it.Key())
			require.Equal(t, all[i].Val, it.Val())

			ok = kv.Seek(ctx, it, all[i-off].Key)
			require.True(t, ok)
			require.Equal(t, all[i-off].Key, it.Key())
			require.Equal(t, all[i-off].Val, it.Val())

			td.ExpectIt(it, all[i-off+1:])
		}
	}
	// regular reset still works
	it.Reset()
	td.ExpectIt(it, all)
}

func increment(t testing.TB, db kv.KV) {
	td := NewTest(t, db)

	key := kv.Key{[]byte("a")}
	td.Put(key, []byte("0"))

	const n = 10
	ready := make(chan struct{})
	errc := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			<-ready
			err := kv.Update(ctx, db, func(tx kv.Tx) error {
				val, err := tx.Get(ctx, key)
				if err != nil {
					return err
				}

				v, err := strconv.Atoi(string(val))
				if err != nil {
					return err
				}

				v++
				val = []byte(strconv.Itoa(v))

				return tx.Put(key, val)
			})
			if err != nil {
				errc <- err
			}
		}()
	}

	close(ready)
	wg.Wait()

	select {
	case err := <-errc:
		require.NoError(t, err)
	default:
	}

	td.Expect(key, []byte("10"))
}
