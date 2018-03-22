package btree

import (
	"testing"

	"github.com/nwca/hidalgo/kv"
	"github.com/nwca/hidalgo/kv/flat"
	"github.com/nwca/hidalgo/kv/kvtest"
)

func TestBtree(t *testing.T) {
	kvtest.RunTest(t, func(t testing.TB) (kv.KV, func()) {
		return flat.New(New()), func() {}
	})
}
