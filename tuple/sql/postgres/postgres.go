package postgres

import (
	"strconv"

	_ "github.com/lib/pq"

	"github.com/hidal-go/hidalgo/base"
	sqltuple "github.com/hidal-go/hidalgo/tuple/sql"
	"github.com/lib/pq"
)

const Name = "postgres"

func init() {
	sqltuple.Register(sqltuple.Registration{
		Registration: base.Registration{
			Name: Name, Title: "PostgreSQL",
			Local: false, Volatile: false,
		},
		Driver: "postgres",
		DSN: func(addr string, ns string) (string, error) {
			return addr + "/" + ns + "?sslmode=disable", nil
		},
		Dialect: sqltuple.Dialect{
			BytesType:               "BYTEA",
			AutoType:                "BIGSERIAL",
			QuoteIdentifierFunc:     pq.QuoteIdentifier,
			DefaultSchema:           "public",
			Unsigned:                false,
			Returning:               true,
			OnConflict:              true,
			NoIteratorsWhenMutating: true,
			Placeholder: func(i int) string {
				return "$" + strconv.Itoa(i+1)
			},
			Errors: func(err error) error {
				return err
			},
			ListColumns: `SELECT c.column_name, c.data_type, c.is_nullable, tc.constraint_type, col_description(a.attrelid, a.attnum)
FROM information_schema.columns c
  LEFT JOIN information_schema.constraint_column_usage AS ccu ON c.table_schema = ccu.table_schema AND c.table_name = ccu.table_name AND c.column_name = ccu.column_name
  LEFT JOIN information_schema.table_constraints AS tc ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND tc.constraint_name = ccu.constraint_name
  LEFT JOIN pg_catalog.pg_attribute AS a ON a.attname = c.column_name
  LEFT JOIN pg_catalog.pg_class AS pc ON a.attrelid = pc.oid AND pc.relname = c.table_name
WHERE c.table_schema = $1
      AND c.table_name = $2
      AND a.attnum > 0
      AND a.attisdropped is false
      AND pg_catalog.pg_table_is_visible(pc.oid)`,
			ColumnCommentSet: func(b *sqltuple.Builder, tbl, col, s string) {
				b.Write(`COMMENT ON COLUMN `)
				b.Idents(tbl)
				b.Write(".")
				b.Idents(col)
				b.Write(` IS `)
				b.Literal(s)
			},
		},
	})
}
