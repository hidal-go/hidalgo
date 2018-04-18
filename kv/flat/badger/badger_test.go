package badger

import (
	"testing"

	"github.com/nwca/hidalgo/kv/flat"
	"github.com/nwca/hidalgo/kv/kvtest"
)

func TestBadger(t *testing.T) {
	kvtest.RunTestLocal(t, flat.UpgradeOpenPath(OpenPath))
}
