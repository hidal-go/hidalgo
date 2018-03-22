package leveldb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/nwca/hidalgo/kv"
	"github.com/nwca/hidalgo/kv/flat"
	"github.com/nwca/hidalgo/kv/kvtest"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func create(t testing.TB) (flat.KV, func()) {
	dir, err := ioutil.TempDir("", "dal-leveldb-")
	require.NoError(t, err)
	db, err := Open(dir, &opt.Options{NoSync: true})
	if err != nil {
		os.RemoveAll(dir)
		require.NoError(t, err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestLeveldb(t *testing.T) {
	kvtest.RunTest(t, func(t testing.TB) (kv.KV, func()) {
		db, closer := create(t)
		return flat.New(db), closer
	})
}
