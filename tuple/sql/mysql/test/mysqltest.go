package mysqltest

import (
	"testing"

	"github.com/ory/dockertest"

	sqltuple "github.com/hidal-go/hidalgo/tuple/sql"
	"github.com/hidal-go/hidalgo/tuple/sql/mysql"
	"github.com/hidal-go/hidalgo/tuple/sql/sqltest"
)

var versions = []string{
	"5.7",
}

func init() {
	var vers []sqltest.Version
	for _, v := range versions {
		vers = append(vers, sqltest.Version{
			Name: v, Factory: MySQLVersion(v),
		})
	}
	sqltest.Register(mysql.Name, vers...)
}

func MySQLVersion(vers string) sqltest.Database {
	const image = "mysql"
	return sqltest.Database{
		Recreate: false,
		Run: func(t testing.TB) string {
			pool, err := dockertest.NewPool("")
			if err != nil {
				t.Fatal(err)
			}

			cont, err := pool.Run(image, vers, []string{
				"MYSQL_ROOT_PASSWORD=root",
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = cont.Close()
			})

			const port = "3306/tcp"
			addr := "root:root@(" + cont.GetHostPort(port) + ")"

			err = pool.Retry(func() error {
				cli, err := sqltuple.OpenSQL(mysql.Name, addr, "")
				if err != nil {
					return err
				}
				defer cli.Close()
				return cli.Ping()
			})
			if err != nil {
				t.Fatal(err)
			}
			return addr
		},
	}
}
