package sqltuple

import (
	"sort"

	"github.com/hidal-go/hidalgo/base"
)

// DSNFunc is a function for building a Data Source Name for SQL driver, given a address and the database name.
type DSNFunc func(addr, ns string) (string, error)

// Registration is an information about the database driver.
type Registration struct {
	base.Registration
	Driver  string
	DSN     DSNFunc
	Dialect Dialect
}

var registry = make(map[string]Registration)

// Register globally registers a database driver.
func Register(reg Registration) {
	if reg.Name == "" {
		panic("name cannot be empty")
	} else if _, ok := registry[reg.Name]; ok {
		panic(base.ErrRegistered{Name: reg.Name})
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
