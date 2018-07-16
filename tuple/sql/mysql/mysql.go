package mysql

import (
	_ "github.com/go-sql-driver/mysql"

	"github.com/go-sql-driver/mysql"
	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/tuple/sql"
)

const Name = "mysql"

func init() {
	sqltuple.Register(sqltuple.Registration{
		Registration: base.Registration{
			Name: Name, Title: "MySQL",
			Local: false, Volatile: false,
		},
		Driver: "mysql",
		DSN: func(addr string, ns string) (string, error) {
			return addr + "/" + ns + "?parseTime=true", nil
		},
		Dialect: sqltuple.Dialect{
			Errors: func(err error) error {
				if e, ok := err.(*mysql.MySQLError); ok {
					switch e.Number {
					case 1146:
						return sqltuple.ErrTableNotFound
					}
				}
				return err
			},
		},
	})
}
