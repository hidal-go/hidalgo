package kv_test

import (
	"testing"

	"github.com/nwca/hidalgo/kv/flat"
	"github.com/nwca/hidalgo/kv/flat/btree"
	"github.com/nwca/hidalgo/tuple"
	"github.com/nwca/hidalgo/tuple/kv"
	"github.com/nwca/hidalgo/tuple/tupletest"
)

func TestKV2Tuple(t *testing.T) {
	tupletest.RunTest(t, func(t testing.TB) (tuple.Store, func()) {
		kdb := btree.New()
		db := kv.New(flat.New(kdb))
		return db, func() {}
	})
}
