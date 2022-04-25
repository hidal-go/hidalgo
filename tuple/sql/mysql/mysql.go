package mysql

import (
	"strings"

	"github.com/go-sql-driver/mysql"   // This import will be dropped if mysql.MySQLError is removed.
	_ "github.com/go-sql-driver/mysql" // This side-effect import must be kept.

	"github.com/hidal-go/hidalgo/base"
	sqltuple "github.com/hidal-go/hidalgo/tuple/sql"
)

const Name = "mysql"

func init() {
	sqltuple.Register(sqltuple.Registration{
		Registration: base.Registration{
			Name: Name, Title: "MySQL",
			Local: false, Volatile: false,
		},
		Driver: "mysql",
		DSN: func(addr, ns string) (string, error) {
			return addr + "/" + ns + "?parseTime=true", nil
		},
		Dialect: sqltuple.Dialect{
			StringType: "TEXT",
			BytesType:  "BLOB",
			// TODO: pick size based on the number of columns (max 3k)
			StringKeyType: "VARCHAR(256)",
			BytesKeyType:  "VARBINARY(256)",
			TimeType:      "DATETIME(6)",
			// TODO: set it on the table/database
			StringTypeCollation:     " CHARACTER SET utf8 COLLATE utf8_unicode_ci",
			Unsigned:                true,
			ReplaceStmt:             true,
			NoIteratorsWhenMutating: true,
			ListColumns: `SELECT column_name, column_type, is_nullable, column_key, column_comment
FROM information_schema.columns WHERE table_schema = ? AND table_name = ?`,
			QuoteIdentifierFunc: func(s string) string {
				return "`" + strings.Replace(s, "`", "", -1) + "`"
			},
			ColumnCommentInline: func(s string) string {
				return "COMMENT " + s
			},
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
