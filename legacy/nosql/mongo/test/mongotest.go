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
		Run: func(tb testing.TB) nosql.Database {
			pool, err := dockertest.NewPool("")
			if err != nil {
				tb.Fatal(err)
			}

			cont, err := pool.Run("mongo", vers, nil)
			if err != nil {
				tb.Fatal(err)
			}
			tb.Cleanup(func() {
				_ = cont.Close()
			})

			addr := fmt.Sprintf("mongodb://%s", cont.GetHostPort("27017/tcp"))
			err = pool.Retry(func() error {
				sess, err := gomongo.NewClient(options.Client().ApplyURI(addr))
				if err != nil {
					return err
				}
				defer sess.Disconnect(context.TODO())

				err = sess.Connect(context.TODO())

				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				tb.Fatal(err)
			}
			qs, err := mongo.Dial(addr, "test", nil)
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
