package bolt

import (
	"path/filepath"
	"testing"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/kvtest"
)

func TestBolt(t *testing.T) {
	kvtest.RunTestLocal(t, func(path string) (kv.KV, error) {
		path = filepath.Join(path, "bolt.db")
		return OpenPath(path)
	}, nil)
}
