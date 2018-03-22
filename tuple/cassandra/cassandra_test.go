package cassandra

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/nwca/hidalgo/tuple"
	"github.com/nwca/hidalgo/tuple/tupletest"
	"github.com/nwca/hidalgo/utils/dock"
	"github.com/stretchr/testify/require"
)

func create(t testing.TB) (tuple.Store, func()) {
	var conf dock.Config
	conf.Image = `cassandra`
	addr, closer := dock.RunAndWait(t, conf, "9042", nil)
	c := gocql.NewCluster(addr)
	c.Timeout = time.Minute
	sess, err := c.CreateSession()
	if err != nil {
		closer()
		require.NoError(t, err)
	}
	err = sess.Query(`CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 1 };`).Exec()
	sess.Close()
	if err != nil {
		closer()
		require.NoError(t, err)
	}
	c.Keyspace = "test"
	sess, err = c.CreateSession()
	if err != nil {
		closer()
		require.NoError(t, err)
	}
	return New(sess, c.Keyspace), func() {
		sess.Close()
		closer()
	}
}

func TestCassandra(t *testing.T) {
	tupletest.RunTest(t, create)
}
