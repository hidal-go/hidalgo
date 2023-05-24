package elastictest

import (
	"context"
	"testing"

	"github.com/ory/dockertest"
	edriver "gopkg.in/olivere/elastic.v5"

	"github.com/hidal-go/hidalgo/legacy/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql/elastic"
	"github.com/hidal-go/hidalgo/legacy/nosql/nosqltest"
)

var versions = []string{
	"6.2.4",
}

func init() {
	var vers []nosqltest.Version
	for _, v := range versions {
		vers = append(vers, nosqltest.Version{
			Name: v, Factory: ElasticVersion(v),
		})
	}
	nosqltest.Register(elastic.Name, vers...)
}

func ElasticVersion(vers string) nosqltest.Database {
	return nosqltest.Database{
		Traits: elastic.Traits(),
		Run: func(t testing.TB) nosql.Database {
			name := "docker.elastic.co/elasticsearch/elasticsearch"

			pool, err := dockertest.NewPool("")
			if err != nil {
				t.Fatal(err)
			}

			cont, err := pool.Run(name, vers, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = cont.Close()
			})

			// Running this command might be necessary on the host:
			// sysctl -w vm.max_map_count=262144

			const port = "9200/tcp"
			addr := "http://" + cont.GetHostPort(port)
			ctx := context.Background()

			err = pool.Retry(func() error {
				cli, err := edriver.NewClient(edriver.SetURL(addr))
				if err != nil {
					return err
				}
				_, _, err = cli.Ping(addr).Do(ctx)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}

			db, err := elastic.Dial(ctx, addr, "test", nil)
			if err != nil {
				t.Fatal(addr, err)
			}
			t.Cleanup(func() {
				_ = db.Close()
			})
			return db
		},
	}
}
