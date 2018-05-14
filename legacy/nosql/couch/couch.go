// +build !js

package couch

import (
	_ "github.com/go-kivik/couchdb" // The CouchDB driver
	"github.com/nwca/hidalgo/base"
	"github.com/nwca/hidalgo/legacy/nosql"
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

func CreateCouch(addr string, ns string, opt nosql.Options) (nosql.Database, error) {
	return Dial(true, DriverCouch, addr, ns, opt)
}

func OpenCouch(addr string, ns string, opt nosql.Options) (nosql.Database, error) {
	return Dial(false, DriverCouch, addr, ns, opt)
}
