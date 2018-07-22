package postgres_test

import (
	"testing"

	_ "github.com/hidal-go/hidalgo/tuple/sql/postgres/test"

	"github.com/hidal-go/hidalgo/tuple/sql/postgres"
	"github.com/hidal-go/hidalgo/tuple/sql/sqltest"
)

func TestPostgreSQL(t *testing.T) {
	sqltest.Test(t, postgres.Name)
}

func BenchmarkPostgreSQL(b *testing.B) {
	sqltest.Benchmark(b, postgres.Name)
}
