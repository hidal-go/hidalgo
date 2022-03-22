package nosqltest

import (
	"sort"
	"testing"

	"github.com/hidal-go/hidalgo/legacy/nosql"
)

type Registration struct {
	nosql.Registration
	Versions []Version
}

type Version struct {
	Name    string
	Factory Database
}

var registry = make(map[string][]Version)

// Register globally registers a database driver.
func Register(name string, vers ...Version) {
	if name == "" {
		panic("name cannot be empty")
	} else if len(vers) == 0 {
		panic("at least one version should be specified")
	} else if r := nosql.ByName(name); r == nil {
		panic("name is not registered")
	}
	vers = append([]Version{}, vers...)
	sort.Slice(vers, func(i, j int) bool {
		return vers[i].Name < vers[j].Name
	})
	registry[name] = vers
}

// List enumerates all globally registered database drivers.
func List() []Registration {
	out := make([]Registration, 0, len(registry))
	for name, vers := range registry {
		out = append(out, Registration{
			Registration: *nosql.ByName(name),
			Versions:     append([]Version{}, vers...),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

// ByName returns a registered database driver by it's name.
func ByName(name string) *Registration {
	vers, ok := registry[name]
	if !ok {
		return nil
	}
	return &Registration{
		Registration: *nosql.ByName(name),
		Versions:     append([]Version{}, vers...),
	}
}

func allNames() []string {
	var names []string
	for name := range registry {
		names = append(names, name)
	}
	return names
}

func RunTest(t *testing.T, test func(t *testing.T, run Database), names ...string) {
	for _, name := range names {
		if _, ok := registry[name]; !ok {
			panic("not registered: " + name)
		}
	}
	run := func(t *testing.T, name string) {
		for _, v := range ByName(name).Versions {
			t.Run(v.Name, func(t *testing.T) {
				test(t, v.Factory)
			})
		}
	}
	if len(names) == 1 {
		run(t, names[0])
		return
	}
	if len(names) == 0 {
		names = allNames()
	}
	for _, name := range names {
		name := name
		t.Run(name, func(t *testing.T) {
			run(t, name)
		})
	}
}

func RunBenchmark(b *testing.B, bench func(b *testing.B, run Database), names ...string) {
	for _, name := range names {
		if _, ok := registry[name]; !ok {
			panic("not registered: " + name)
		}
	}
	run := func(_ *testing.B, name string) {
		for _, v := range ByName(name).Versions {
			b.Run(v.Name, func(_ *testing.B) {
				bench(b, v.Factory)
			})
		}
	}
	if len(names) == 1 {
		run(b, names[0])
		return
	}
	if len(names) == 0 {
		names = allNames()
	}
	for _, name := range names {
		name := name
		b.Run(name, func(_ *testing.B) {
			run(b, name)
		})
	}
}

func Test(t *testing.T, names ...string) {
	RunTest(t, TestNoSQL, names...)
}

func Benchmark(b *testing.B, names ...string) {
	RunBenchmark(b, BenchmarkNoSQL, names...)
}
