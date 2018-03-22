package bolt

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/nwca/hidalgo/kv"
	"github.com/nwca/hidalgo/kv/kvtest"
	"github.com/stretchr/testify/require"
)

func create(t testing.TB) (kv.KV, func()) {
	dir, err := ioutil.TempDir("", "dal-leveldb-")
	require.NoError(t, err)
	db, err := Open(filepath.Join(dir, "db.bolt"), &bolt.Options{Timeout: time.Second * 10})
	if err != nil {
		os.RemoveAll(dir)
		require.NoError(t, err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestBolt(t *testing.T) {
	kvtest.RunTest(t, create)
}
