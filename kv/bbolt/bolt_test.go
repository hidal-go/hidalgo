package bbolt_test

import (
	"path/filepath"
	"testing"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/bbolt"
	"github.com/hidal-go/hidalgo/kv/kvtest"
)

func TestBBolt(t *testing.T) {
	kvtest.RunTestLocal(t, func(path string) (kv.KV, error) {
		path = filepath.Join(path, "bbolt.db")
		return bbolt.OpenPath(path)
	}, nil)
}
