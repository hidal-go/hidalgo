package tupletest

import (
	"testing"

	"github.com/hidal-go/hidalgo/tuple"
)

func NewTest(tb testing.TB, db tuple.Store) *Test {
	return &Test{t: tb, db: db}
}

type Test struct {
	t  testing.TB
	db tuple.Store
}
