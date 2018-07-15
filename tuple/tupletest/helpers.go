package tupletest

import (
	"testing"

	"github.com/hidal-go/hidalgo/tuple"
)

func NewTest(t testing.TB, db tuple.Store) *Test {
	return &Test{t: t, db: db}
}

type Test struct {
	t  testing.TB
	db tuple.Store
}
