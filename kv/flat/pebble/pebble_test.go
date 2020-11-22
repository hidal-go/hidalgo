package pebble

import (
	"testing"

	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/kvtest"
)

func TestPebble(t *testing.T) {
	kvtest.RunTestLocal(t, flat.UpgradeOpenPath(OpenPath))
}
