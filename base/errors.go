package base

import "fmt"

// ErrVolatile is returned when trying to pass a path for opening an in-memory database.
var ErrVolatile = fmt.Errorf("database is in-memory")

var _ error = ErrRegistered{}

// ErrRegistered is thrown when trying to register a database driver with a name that is already registered.
type ErrRegistered struct {
	Name string
}

func (e ErrRegistered) Error() string {
	return "already registered: " + e.Name
}
