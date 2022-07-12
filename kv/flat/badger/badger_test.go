package badger_test

import (
	"testing"

	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/flat/badger"
	"github.com/hidal-go/hidalgo/kv/kvtest"
)

func TestBadger(t *testing.T) {
	kvtest.RunTestLocal(t, flat.UpgradeOpenPath(badger.OpenPath), nil)
}
