package kv_test

import (
	"testing"

	"github.com/nwca/uda/kv/flat"
	"github.com/nwca/uda/kv/flat/btree"
	"github.com/nwca/uda/tuple"
	"github.com/nwca/uda/tuple/kv"
	"github.com/nwca/uda/tuple/tupletest"
)

func TestKV2Tuple(t *testing.T) {
	tupletest.RunTest(t, func(t testing.TB) (tuple.Store, func()) {
		kdb := btree.New()
		db := kv.New(flat.New(kdb))
		return db, func() {}
	})
}
