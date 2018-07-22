package sqltest

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/tuple/sql"
	"github.com/hidal-go/hidalgo/tuple/tupletest"
	"github.com/stretchr/testify/require"
)

type Database struct {
	Recreate bool
	Run      func(t testing.TB) (string, func())
}

func TestSQL(t *testing.T, name string, gen Database) {
	var (
		addr string
	)
	recreate := gen.Recreate
	if !recreate {
		var closer func()
		addr, closer = gen.Run(t)
		defer closer()
	}
	tupletest.RunTest(t, func(t testing.TB) (tuple.Store, func()) {
		db := fmt.Sprintf("db_%x", rand.Int())
		addr := addr
		destroy := func() {}
		if recreate {
			addr, destroy = gen.Run(t)
		}
		conn, err := sqltuple.OpenSQL(name, addr, "")
		if err != nil {
			destroy()
			require.NoError(t, err)
		}
		_, err = conn.Exec(`CREATE DATABASE ` + db)
		conn.Close()
		if err != nil {
			destroy()
			require.NoError(t, err)
		}
		conn, err = sqltuple.OpenSQL(name, addr, db)
		if err != nil {
			destroy()
			require.NoError(t, err)
		}
		s := sqltuple.New(conn, db, sqltuple.ByName(name).Dialect)
		return s, func() {
			conn.Close()
			if !recreate {
				conn, err := sqltuple.OpenSQL(name, addr, "")
				if err == nil {
					_, err = conn.Exec(`DROP DATABASE ` + db)
					conn.Close()
				}
				if err != nil {
					t.Errorf("cannot remove test database: %v", err)
				}
			}
			destroy()
		}
	})
}

func BenchmarkSQL(t *testing.B, gen Database) {
	// TODO
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
