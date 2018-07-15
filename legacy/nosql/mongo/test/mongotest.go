package mongotest

import (
	"testing"

	"github.com/ory/dockertest"
	"gopkg.in/mgo.v2"

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

			addr := cont.GetHostPort("27017/tcp")

			err = pool.Retry(func() error {
				sess, err := mgo.Dial(addr)
				if err != nil {
					return err
				}
				sess.Close()
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
