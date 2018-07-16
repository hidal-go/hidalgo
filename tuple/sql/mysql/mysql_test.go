package mysql_test

import (
	"testing"

	_ "github.com/hidal-go/hidalgo/tuple/sql/mysql/test"

	"github.com/hidal-go/hidalgo/tuple/sql/mysql"
	"github.com/hidal-go/hidalgo/tuple/sql/sqltest"
)

func TestMySQL(t *testing.T) {
	sqltest.Test(t, mysql.Name)
}

func BenchmarkMySQL(b *testing.B) {
	sqltest.Benchmark(b, mysql.Name)
}
