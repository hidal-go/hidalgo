// +build js

package couch

import (
	"context"

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/legacy/nosql"

	_ "github.com/go-kivik/pouchdb" // The PouchDB driver
)

const (
	NamePouch   = "pouch"
	DriverPouch = "couch"
)

func init() {
	nosql.Register(nosql.Registration{
		Registration: base.Registration{
			Name: NamePouch, Title: DriverPouch,
			Local: true, Volatile: false,
		},
		Traits: Traits(),
		New:    CreatePouch, Open: OpenPouch,
	})
}

func CreatePouch(ctx context.Context, addr string, ns string, opt nosql.Options) (nosql.Database, error) {
	return Dial(ctx, true, DriverPouch, addr, ns, opt)
}

func OpenPouch(ctx context.Context, addr string, ns string, opt nosql.Options) (nosql.Database, error) {
	return Dial(ctx, false, DriverPouch, addr, ns, opt)
}
