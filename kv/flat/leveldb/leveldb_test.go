package leveldb_test

import (
	"testing"

	"github.com/hidal-go/hidalgo/kv/flat"
	"github.com/hidal-go/hidalgo/kv/flat/leveldb"
	"github.com/hidal-go/hidalgo/kv/kvtest"
)

func TestLeveldb(t *testing.T) {
	kvtest.RunTestLocal(t, flat.UpgradeOpenPath(leveldb.OpenPath), nil)
}
