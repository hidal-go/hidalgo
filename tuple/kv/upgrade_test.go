package tuplekv_test

import (
	"testing"

	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/flat/btree"
	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/tuple/kv"
	"github.com/hidal-go/hidalgo/tuple/tupletest"
)

func TestKV2Tuple(t *testing.T) {
	tupletest.RunTest(t, func(t testing.TB) tuple.Store {
		kdb := btree.New()
		db := tuplekv.New(flat.Upgrade(kdb))
		return db
	}, &tupletest.Options{
		NoLocks: true,
	})
}
