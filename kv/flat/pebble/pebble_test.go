package pebble_test

import (
	"testing"

	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/flat/pebble"
	"github.com/hidal-go/hidalgo/kv/kvtest"
)

func TestPebble(t *testing.T) {
	kvtest.RunTestLocal(t, flat.UpgradeOpenPath(pebble.OpenPath), &kvtest.Options{
		NoTx: true,
	})
}
