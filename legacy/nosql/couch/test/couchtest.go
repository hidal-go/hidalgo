//go:build !js
// +build !js

package couchtest

import (
	"context"
	"testing"

	"github.com/hidal-go/hidalgo/legacy/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql/couch"
	"github.com/hidal-go/hidalgo/legacy/nosql/nosqltest"
	"github.com/ory/dockertest"
)

func init() {
	const vers = "2"
	nosqltest.Register(couch.NameCouch, nosqltest.Version{
		Name: vers, Factory: CouchVersion(vers),
	})
}

func CouchVersion(vers string) nosqltest.Database {
	return nosqltest.Database{
		Traits: couch.Traits(),
		Run: func(t testing.TB) (nosql.Database, func()) {
			pool, err := dockertest.NewPool("")
			if err != nil {
				t.Fatal(err)
			}

			cont, err := pool.Run("couchdb", vers, []string{
				"COUCHDB_USER=test",
				"COUCHDB_PASSWORD=test",
			})
			if err != nil {
				t.Fatal(err)
			}
			defer cont.Close()

			ctx := context.Background()

			addr := cont.GetHostPort("5984/tcp")
			addr = "http://test:test@" + addr + "/test"
			_ = pool.Retry(func() error {
				cli, _, err := couch.DialDriver(ctx, couch.DriverCouch, addr, "test")
				if err != nil {
					t.Fatal(err)
				}
				_, err = cli.Version(ctx)
				if err != nil {
					t.Fatal(err)
				}
				return nil
			})

			qs, err := couch.Dial(true, couch.DriverCouch, addr, "test", nil)
			if err != nil {
				t.Fatal(err)
			}
			return qs, func() {
				qs.Close()
			}
		},
	}
}
