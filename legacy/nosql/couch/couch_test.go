//go:build !js
// +build !js

package couch_test

import (
	"testing"

	"github.com/hidal-go/hidalgo/legacy/nosql/couch"
	_ "github.com/hidal-go/hidalgo/legacy/nosql/couch/test"
	"github.com/hidal-go/hidalgo/legacy/nosql/nosqltest"
)

func TestCouch(t *testing.T) {
	nosqltest.Test(t, couch.NameCouch)
}

func BenchmarkCouch(b *testing.B) {
	nosqltest.Benchmark(b, couch.NameCouch)
}
