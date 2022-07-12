package flat

import (
	"sort"

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/kv"
)

// OpenPathFunc is a function for opening a database given a path.
type OpenPathFunc func(path string) (KV, error)

// Registration is an information about the database driver.
type Registration struct {
	OpenPath OpenPathFunc
	base.Registration
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

	// Register as a hierarchical KV implementation
	reg.Name = "flat" + base.RegistrySep + reg.Name
	kv.Register(kv.Registration{
		Registration: reg.Registration,
		OpenPath: func(path string) (kv.KV, error) {
			flat, err := reg.OpenPath(path)
			if err != nil {
				return nil, err
			}
			return Upgrade(flat), nil
		},
	})
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
