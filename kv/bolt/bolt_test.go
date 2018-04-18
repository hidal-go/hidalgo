package bolt

import (
	"path/filepath"
	"testing"

	"github.com/nwca/hidalgo/kv"
	"github.com/nwca/hidalgo/kv/kvtest"
)

func TestBolt(t *testing.T) {
	kvtest.RunTestLocal(t, func(path string) (kv.KV, error) {
		path = filepath.Join(path, "bolt.db")
		return OpenPath(path)
	})
}
