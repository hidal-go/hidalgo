package datastore

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/tuple/tupletest"
	"github.com/ory/dockertest"
)

func TestDatastore(t *testing.T) {
	tupletest.RunTest(t, func(t testing.TB) (tuple.Store, func()) {
		pool, err := dockertest.NewPool("")
		if err != nil {
			t.Fatal(err)
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
			t.Fatal(err)
		}

		ctx := context.Background()
		host := cont.GetHostPort("8080/tcp")
		if host == "" {
			t.Fatal("empty host")
		}
		if err = os.Setenv("DATASTORE_EMULATOR_HOST", host); err != nil {
			t.Fatal(err)
		} else if host := os.Getenv("DATASTORE_EMULATOR_HOST"); host == "" {
			t.Fatal("set env failed")
		}
		cli, err := datastore.NewClient(ctx, proj)
		if err != nil {
			cont.Close()
			t.Fatal(err)
		}
		return OpenClient(cli), func() {
			cli.Close()
			cont.Close()
		}
	})
}
