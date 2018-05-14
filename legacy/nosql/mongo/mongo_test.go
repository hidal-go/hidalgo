package mongo_test

import (
	"testing"

	"github.com/nwca/hidalgo/legacy/nosql/mongo"
	_ "github.com/nwca/hidalgo/legacy/nosql/mongo/test"
	"github.com/nwca/hidalgo/legacy/nosql/nosqltest"
)

func TestMongo(t *testing.T) {
	nosqltest.Test(t, mongo.Name)
}

func BenchmarkMongo(b *testing.B) {
	nosqltest.Benchmark(b, mongo.Name)
}
