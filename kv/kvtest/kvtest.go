package kvtest

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/stretchr/testify/require"
)

// Func is a constructor for database implementations.
// It returns an empty database and a function to destroy it.
type Func func(t testing.TB) (kv.KV, func())

// RunTest runs all tests for key-value implementations.
func RunTest(t *testing.T, fnc Func) {
	for _, c := range testList {
		t.Run(c.name, func(t *testing.T) {
			db, closer := fnc(t)
			defer closer()
			c.test(t, db)
		})
	}
}

// RunTestLocal is a wrapper for RunTest that automatically creates a temporary directory and opens a database.
func RunTestLocal(t *testing.T, open kv.OpenPathFunc) {
	RunTest(t, func(t testing.TB) (kv.KV, func()) {
		dir, err := ioutil.TempDir("", "dal-kv-")
		require.NoError(t, err)

		db, err := open(dir)
		if err != nil {
			os.RemoveAll(dir)
			require.NoError(t, err)
		}
		return db, func() {
			db.Close()
			os.RemoveAll(dir)
		}
	})
}

var testList = []struct {
	name string
	test func(t testing.TB, db kv.KV)
}{
	{name: "basic", test: basic},
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

	td.Scan(nil, all)
	td.Scan(keys[0], all[:1])
	td.Scan(keys[len(keys)-1], all[len(all)-1:])
	td.Scan(keys[1][:1], all[1:len(all)-1])
	td.Scan(kv.Key{keys[1][0], keys[1][1][:1]}, all[1:4])

	for _, k := range keys {
		td.Del(k)
	}
	for _, k := range keys {
		td.NotExists(k)
	}
}
