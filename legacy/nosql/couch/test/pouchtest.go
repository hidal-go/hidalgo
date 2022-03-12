// +build js

package couchtest

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/go-kivik/kivik"
	"github.com/hidal-go/hidalgo/legacy/nosql"
	"github.com/hidal-go/hidalgo/legacy/nosql/couch"
	"github.com/hidal-go/hidalgo/legacy/nosql/nosqltest"
)

func init() {
	nosqltest.Register(couch.NamePouch, nosqltest.Version{
		Name: "pouch", Factory: Pouch(),
	})
}

func Pouch() nosqltest.Database {
	return nosqltest.Database{
		Traits: couch.Traits(),
		Run: func(t testing.TB) (nosql.Database, func()) {
			dir, err := ioutil.TempDir("", "pouch-")
			if err != nil {
				t.Fatal("failed to make temp dir:", err)
			}

			name := fmt.Sprintf("db-%d", rand.Int())

			qs, err := couch.Dial(false, couch.DriverPouch, dir+"/"+name, name, nil)
			if err != nil {
				os.RemoveAll(dir)
				t.Fatal(err)
			}

			return qs, func() {
				qs.Close()
				ctx := context.TODO()
				if c, err := kivik.New(couch.DriverPouch, dir); err == nil {
					_ = c.DestroyDB(ctx, name)
				}
				if err := os.RemoveAll(dir); err != nil { // remove the test data
					t.Fatal(err)
				}
			}
		},
	}
}
