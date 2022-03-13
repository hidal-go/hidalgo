package elastic_test

import (
	"testing"

	"github.com/hidal-go/hidalgo/legacy/nosql/elastic"
	_ "github.com/hidal-go/hidalgo/legacy/nosql/elastic/test"
	"github.com/hidal-go/hidalgo/legacy/nosql/nosqltest"
)

func TestElastic(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	nosqltest.Test(t, elastic.Name)
}

func BenchmarkElastic(b *testing.B) {
	nosqltest.Benchmark(b, elastic.Name)
}
