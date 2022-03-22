package nosql

import (
	"sort"

	"github.com/hidal-go/hidalgo/base"
)

// OpenFunc is a function for opening a database given a address and the database name.
type OpenFunc func(addr, ns string, opt Options) (Database, error)

// Registration is an information about the database driver.
type Registration struct {
	base.Registration
	New, Open OpenFunc
	Traits    Traits
}

var registry = make(map[string]Registration)

// Register globally registers a database driver.
func Register(reg Registration) {
	if reg.Name == "" {
		panic("name cannot be empty")
	} else if _, ok := registry[reg.Name]; ok {
		panic(base.ErrRegistered{Name: reg.Name})
	}
	if reg.New == nil {
		reg.New = reg.Open
	}
	if reg.Open == nil {
		reg.Open = reg.New
	}
	registry[reg.Name] = reg
}

// List enumerates all globally registered database drivers.
func List() []Registration {
	out := make([]Registration, 0, len(registry))
	for _, r := range registry {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

// ByName returns a registered database driver by it's name.
func ByName(name string) *Registration {
	r, ok := registry[name]
	if !ok {
		return nil
	}
	return &r
}
