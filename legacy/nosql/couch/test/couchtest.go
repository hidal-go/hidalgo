//go:build !js
// +build !js

package couchtest

import (
	"context"
	"testing"

	"github.com/ory/dockertest"

	"github.com/hidal-go/hidalgo/legacy/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql/couch"
	"github.com/hidal-go/hidalgo/legacy/nosql/nosqltest"
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
		Run: func(tb testing.TB) nosql.Database {
			pool, err := dockertest.NewPool("")
			if err != nil {
				tb.Fatal(err)
			}

			cont, err := pool.Run("couchdb", vers, []string{
				"COUCHDB_USER=test",
				"COUCHDB_PASSWORD=test",
			})
			if err != nil {
				tb.Fatal(err)
			}
			tb.Cleanup(func() {
				_ = cont.Close()
			})

			ctx := context.Background()

			addr := cont.GetHostPort("5984/tcp")
			addr = "http://test:test@" + addr + "/test"
			err = pool.Retry(func() error {
				cli, _, err := couch.DialDriver(ctx, couch.DriverCouch, addr, "test")
				if err != nil {
					return err
				}
				_, err = cli.Version(ctx)
				return err
			})
			if err != nil {
				tb.Fatal(err)
			}

			qs, err := couch.Dial(true, couch.DriverCouch, addr, "test", nil)
			if err != nil {
				tb.Fatal(err)
			}
			tb.Cleanup(func() {
				_ = qs.Close()
			})
			return qs
		},
	}
}
