package btree

import (
	"testing"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/kvtest"
)

func TestBtree(t *testing.T) {
	kvtest.RunTest(t, func(t testing.TB) kv.KV {
		return flat.Upgrade(New())
	}, nil)
}
