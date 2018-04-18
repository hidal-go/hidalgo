package badger

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/nwca/hidalgo/kv"
	"github.com/nwca/hidalgo/kv/flat"
	"github.com/nwca/hidalgo/kv/kvtest"
	"github.com/stretchr/testify/require"
)

func create(t testing.TB) (flat.KV, func()) {
	dir, err := ioutil.TempDir("", "dal-badger-")
	require.NoError(t, err)
	opt := badger.DefaultOptions
	opt.Dir = dir
	db, err := Open(opt)
	if err != nil {
		os.RemoveAll(dir)
		require.NoError(t, err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestBadger(t *testing.T) {
	kvtest.RunTest(t, func(t testing.TB) (kv.KV, func()) {
		db, closer := create(t)
		return flat.New(db), closer
	})
}
