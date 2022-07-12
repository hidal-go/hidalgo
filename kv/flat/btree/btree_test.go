package btree_test

import (
	"testing"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/flat/btree"
	"github.com/hidal-go/hidalgo/kv/kvtest"
)

func TestBtree(t *testing.T) {
	kvtest.RunTest(t, func(tb testing.TB) kv.KV {
		return flat.Upgrade(btree.New())
	}, nil)
}
