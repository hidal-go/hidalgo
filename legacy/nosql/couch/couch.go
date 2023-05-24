//go:build !js
// +build !js

package couch

import (
	"context"

	_ "github.com/go-kivik/couchdb" // The CouchDB driver

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/legacy/nosql"
)

const (
	NameCouch   = "couch"
	DriverCouch = "couch"
)

func init() {
	nosql.Register(nosql.Registration{
		Registration: base.Registration{
			Name: NameCouch, Title: "CouchDB",
			Local: false, Volatile: false,
		},
		Traits: Traits(),
		New:    CreateCouch, Open: OpenCouch,
	})
}

func CreateCouch(ctx context.Context, addr, ns string, opt nosql.Options) (nosql.Database, error) {
	return Dial(ctx, true, DriverCouch, addr, ns, opt)
}

func OpenCouch(ctx context.Context, addr, ns string, opt nosql.Options) (nosql.Database, error) {
	return Dial(ctx, false, DriverCouch, addr, ns, opt)
}
