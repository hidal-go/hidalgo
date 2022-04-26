package sqltest

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hidal-go/hidalgo/tuple"
	sqltuple "github.com/hidal-go/hidalgo/tuple/sql"
	"github.com/hidal-go/hidalgo/tuple/tupletest"
)

type Database struct {
	Run      func(tb testing.TB) string
	Recreate bool
}

func TestSQL(t *testing.T, name string, gen Database) {
	var addr string
	recreate := gen.Recreate
	if !recreate {
		addr = gen.Run(t)
	}
	tupletest.RunTest(t, func(tb testing.TB) tuple.Store {
		db := fmt.Sprintf("db_%x", rand.Int())
		addr := addr
		if recreate {
			addr = gen.Run(tb)
		}
		conn, err := sqltuple.OpenSQL(name, addr, "")
		if err != nil {
			require.NoError(tb, err)
		}
		_, err = conn.Exec(`CREATE DATABASE ` + db)
		conn.Close()
		if err != nil {
			require.NoError(tb, err)
		}
		conn, err = sqltuple.OpenSQL(name, addr, db)
		if err != nil {
			require.NoError(tb, err)
		}
		tb.Cleanup(func() {
			conn.Close()
			if !recreate {
				conn, err = sqltuple.OpenSQL(name, addr, "")
				if err == nil {
					_, err = conn.Exec(`DROP DATABASE ` + db)
					conn.Close()
				}
				if err != nil {
					tb.Errorf("cannot remove test database: %v", err)
				}
			}
		})
		return sqltuple.New(conn, db, sqltuple.ByName(name).Dialect)
	}, nil)
}

func BenchmarkSQL(t *testing.B, gen Database) {
	// TODO
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
