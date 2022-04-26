package mysqltest

import (
	"testing"

	"github.com/ory/dockertest"

	sqltuple "github.com/hidal-go/hidalgo/tuple/sql"
	"github.com/hidal-go/hidalgo/tuple/sql/postgres"
	"github.com/hidal-go/hidalgo/tuple/sql/sqltest"
)

var versions = []string{
	"13",
}

func init() {
	var vers []sqltest.Version
	for _, v := range versions {
		vers = append(vers, sqltest.Version{
			Name: v, Factory: PostgresVersion(v),
		})
	}
	sqltest.Register(postgres.Name, vers...)
}

func PostgresVersion(vers string) sqltest.Database {
	const image = "postgres"
	return sqltest.Database{
		Recreate: false,
		Run: func(tb testing.TB) string {
			pool, err := dockertest.NewPool("")
			if err != nil {
				tb.Fatal(err)
			}

			cont, err := pool.Run(image, vers, []string{
				"POSTGRES_PASSWORD=postgres",
			})
			if err != nil {
				tb.Fatal(err)
			}
			tb.Cleanup(func() {
				_ = cont.Close()
			})

			const port = "5432/tcp"
			addr := `postgres://postgres:postgres@` + cont.GetHostPort(port)

			err = pool.Retry(func() error {
				cli, err := sqltuple.OpenSQL(postgres.Name, addr, "")
				if err != nil {
					return err
				}
				defer cli.Close()
				return cli.Ping()
			})
			if err != nil {
				tb.Fatal(err)
			}
			return addr
		},
	}
}
