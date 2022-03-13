package sqltest

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/tuple/sql"
	"github.com/hidal-go/hidalgo/tuple/tupletest"
)

type Database struct {
	Recreate bool
	Run      func(t testing.TB) string
}

func TestSQL(t *testing.T, name string, gen Database) {
	var (
		addr string
	)
	recreate := gen.Recreate
	if !recreate {
		addr = gen.Run(t)
	}
	tupletest.RunTest(t, func(t testing.TB) tuple.Store {
		db := fmt.Sprintf("db_%x", rand.Int())
		addr := addr
		if recreate {
			addr = gen.Run(t)
		}
		conn, err := sqltuple.OpenSQL(name, addr, "")
		if err != nil {
			require.NoError(t, err)
		}
		_, err = conn.Exec(`CREATE DATABASE ` + db)
		conn.Close()
		if err != nil {
			require.NoError(t, err)
		}
		conn, err = sqltuple.OpenSQL(name, addr, db)
		if err != nil {
			require.NoError(t, err)
		}
		t.Cleanup(func() {
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
