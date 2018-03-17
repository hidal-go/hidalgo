package btree

import (
	"testing"

	"github.com/nwca/uda/kv"
	"github.com/nwca/uda/kv/flat"
	"github.com/nwca/uda/kv/kvtest"
)

func TestBtree(t *testing.T) {
	kvtest.RunTest(t, func(t testing.TB) (kv.KV, func()) {
		return flat.New(New(), '/'), func() {}
	})
}
