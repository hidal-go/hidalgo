package kvtest

import (
	"strconv"
	"testing"

	. "github.com/nwca/uda/kv"
)

type Func func(t testing.TB) (KV, func())

func RunTest(t *testing.T, fnc Func) {
	for _, c := range testList {
		t.Run(c.name, func(t *testing.T) {
			db, closer := fnc(t)
			defer closer()
			c.test(t, db)
		})
	}
}

var testList = []struct {
	name string
	test func(t testing.TB, db KV)
}{
	{name: "basic", test: basic},
}

func basic(t testing.TB, db KV) {
	td := NewTest(t, db)

	keys := []Key{
		{[]byte("a")},
		{[]byte("b"), []byte("a")},
		{[]byte("b"), []byte("a1")},
		{[]byte("b"), []byte("a2")},
		{[]byte("b"), []byte("b")},
		{[]byte("c")},
	}

	td.notExists(nil)
	for _, k := range keys {
		td.notExists(k)
	}

	var all []Pair
	for i, k := range keys {
		v := Value(strconv.Itoa(i))
		td.put(k, v)
		td.expect(k, v)
		all = append(all, Pair{Key: k, Val: v})
	}

	td.iterate(nil, all)
	td.iterate(keys[0], all[:1])
	td.iterate(keys[1][:1], all[1:len(all)-1])
	td.iterate(Key{keys[1][0], keys[1][1][:1]}, all[1:4])

	for _, k := range keys {
		td.del(k)
	}
	for _, k := range keys {
		td.notExists(k)
	}
}
