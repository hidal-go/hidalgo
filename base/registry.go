package base

// RegistrySep is a name separator for building implementation hierarchy.
const RegistrySep = "."

// Registration is a common information about the database driver.
type Registration struct {
	Name     string // unique name
	Title    string // human-readable name
	Local    bool   // stores data on local disk or keeps it in-memory
	Volatile bool   // not persistent
}
