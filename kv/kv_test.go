package kv_test

import (
	"testing"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/stretchr/testify/require"
)

var keyCompareCases = []struct {
	k1, k2 kv.Key
	exp    int
}{
	{
		k1:  kv.SKey("a"),
		k2:  kv.SKey("a"),
		exp: 0,
	},
	{
		k1:  kv.SKey("a"),
		k2:  kv.SKey("b"),
		exp: -1,
	},
	{
		k1:  kv.SKey("a"),
		k2:  kv.SKey("a", "b"),
		exp: -1,
	},
	{
		k1:  kv.SKey("a", "b"),
		k2:  kv.SKey("a", "a"),
		exp: +1,
	},
	{
		k1:  nil,
		k2:  kv.SKey("a", "b"),
		exp: -1,
	},
	{
		k1:  kv.SKey("ab"),
		k2:  kv.SKey("a", "b"),
		exp: +1,
	},
}

func TestKeyCompare(t *testing.T) {
	for _, c := range keyCompareCases {
		t.Run("", func(t *testing.T) {
			d := c.k1.Compare(c.k2)
			require.Equal(t, c.exp, d)
			if d != 0 {
				require.Equal(t, -c.exp, c.k2.Compare(c.k1))
			}
		})
	}
}

var keyHasPrefixCases = []struct {
	key, pref kv.Key
	exp       bool
}{
	{
		key:  kv.SKey("a"),
		pref: kv.SKey("a"),
		exp:  true,
	},
	{
		key:  kv.SKey("a"),
		pref: kv.SKey("b"),
		exp:  false,
	},
	{
		key:  kv.SKey("a"),
		pref: kv.SKey("a", "b"),
		exp:  false,
	},
	{
		key:  kv.SKey("a", "b"),
		pref: kv.SKey("a"),
		exp:  true,
	},
	{
		key:  kv.SKey("a", "b"),
		pref: kv.SKey("a", "a"),
		exp:  false,
	},
	{
		key:  nil,
		pref: kv.SKey("a", "b"),
		exp:  false,
	},
	{
		key:  kv.SKey("a", "b"),
		pref: nil,
		exp:  true,
	},
	{
		key:  kv.SKey("ab"),
		pref: kv.SKey("a", "b"),
		exp:  false,
	},
	{
		key:  kv.SKey("a", "b"),
		pref: kv.SKey("ab"),
		exp:  false,
	},
}

func TestKeyHasPrefix(t *testing.T) {
	for _, c := range keyHasPrefixCases {
		t.Run("", func(t *testing.T) {
			d := c.key.HasPrefix(c.pref)
			require.Equal(t, c.exp, d)
		})
	}
}

func TestKeyAppend(t *testing.T) {
	k := kv.SKey("a", "b", "c")
	k = k.Append(kv.SKey("d"))
	require.Equal(t, kv.SKey("a", "b", "c", "d"), k)
	k = kv.SKey("a", "b", "c")
	k = k.AppendBytes([]byte("d"))
	require.Equal(t, kv.SKey("a", "b", "c", "d"), k)
}
