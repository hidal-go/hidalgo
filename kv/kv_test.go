package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var keyCompareCases = []struct {
	k1, k2 Key
	exp    int
}{
	{
		k1:  SKey("a"),
		k2:  SKey("a"),
		exp: 0,
	},
	{
		k1:  SKey("a"),
		k2:  SKey("b"),
		exp: -1,
	},
	{
		k1:  SKey("a"),
		k2:  SKey("a", "b"),
		exp: -1,
	},
	{
		k1:  SKey("a", "b"),
		k2:  SKey("a", "a"),
		exp: +1,
	},
	{
		k1:  nil,
		k2:  SKey("a", "b"),
		exp: -1,
	},
	{
		k1:  SKey("ab"),
		k2:  SKey("a", "b"),
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
	key, pref Key
	exp       bool
}{
	{
		key:  SKey("a"),
		pref: SKey("a"),
		exp:  true,
	},
	{
		key:  SKey("a"),
		pref: SKey("b"),
		exp:  false,
	},
	{
		key:  SKey("a"),
		pref: SKey("a", "b"),
		exp:  false,
	},
	{
		key:  SKey("a", "b"),
		pref: SKey("a"),
		exp:  true,
	},
	{
		key:  SKey("a", "b"),
		pref: SKey("a", "a"),
		exp:  false,
	},
	{
		key:  nil,
		pref: SKey("a", "b"),
		exp:  false,
	},
	{
		key:  SKey("a", "b"),
		pref: nil,
		exp:  true,
	},
	{
		key:  SKey("ab"),
		pref: SKey("a", "b"),
		exp:  false,
	},
	{
		key:  SKey("a", "b"),
		pref: SKey("ab"),
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
	k := SKey("a", "b", "c")
	k = k.Append(SKey("d"))
	require.Equal(t, SKey("a", "b", "c", "d"), k)
	k = SKey("a", "b", "c")
	k = k.AppendBytes([]byte("d"))
	require.Equal(t, SKey("a", "b", "c", "d"), k)
}
