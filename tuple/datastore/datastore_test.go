package datastore_test

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/ory/dockertest"

	"github.com/hidal-go/hidalgo/tuple"
	ds "github.com/hidal-go/hidalgo/tuple/datastore"
	"github.com/hidal-go/hidalgo/tuple/tupletest"
)

func TestDatastore(t *testing.T) {
	tupletest.RunTest(t, func(tb testing.TB) tuple.Store {
		pool, err := dockertest.NewPool("")
		if err != nil {
			tb.Fatal(err)
		}

		const (
			proj = "test"
		)

		cont, err := pool.RunWithOptions(&dockertest.RunOptions{
			Repository: "singularities/datastore-emulator",
			Tag:        "latest",
			Env: []string{
				"DATASTORE_LISTEN_ADDRESS=0.0.0.0:8080",
				"DATASTORE_PROJECT_ID=" + proj,
			},
			ExposedPorts: []string{
				"8080/tcp",
			},
		})
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() {
			_ = cont.Close()
		})

		ctx := context.Background()
		host := cont.GetHostPort("8080/tcp")
		if host == "" {
			tb.Fatal("empty host")
		}

		if err = os.Setenv("DATASTORE_EMULATOR_HOST", host); err != nil {
			tb.Fatal(err)
		} else if host := os.Getenv("DATASTORE_EMULATOR_HOST"); host == "" {
			tb.Fatal("set env failed")
		}
		cli, err := datastore.NewClient(ctx, proj)
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() {
			_ = cli.Close()
		})

		return ds.OpenClient(cli)
	}, nil)
}
