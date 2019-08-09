package mongotest

import (
	"context"
	"fmt"
	"testing"

	"github.com/ory/dockertest"
	gomongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/hidal-go/hidalgo/legacy/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql/mongo"
	"github.com/hidal-go/hidalgo/legacy/nosql/nosqltest"
)

const vers = "3"

func init() {
	nosqltest.Register(mongo.Name, nosqltest.Version{
		Name: vers, Factory: MongoVersion(vers),
	})
}

func MongoVersion(vers string) nosqltest.Database {
	return nosqltest.Database{
		Traits: mongo.Traits(),
		Run: func(t testing.TB) (nosql.Database, func()) {
			pool, err := dockertest.NewPool("")
			if err != nil {
				t.Fatal(err)
			}

			cont, err := pool.Run("mongo", vers, nil)
			if err != nil {
				t.Fatal(err)
			}

			addr := fmt.Sprintf("mongodb://%s", cont.GetHostPort("27017/tcp"))
			err = pool.Retry(func() error {
				sess, err := gomongo.NewClient(options.Client().ApplyURI(addr))

				if err != nil {
					t.Fatal(err)
				}

				if err == nil {
					err = sess.Connect(context.TODO())
				}

				if err != nil {
					return err
				}

				sess.Disconnect(context.TODO())
				return nil
			})
			if err != nil {
				cont.Close()
				t.Fatal(err)
			}
			qs, err := mongo.Dial(addr, "test", nil)
			if err != nil {
				cont.Close()
				t.Fatal(err)
			}
			return qs, func() {
				qs.Close()
				cont.Close()
			}
		},
	}
}
