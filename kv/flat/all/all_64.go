//go:build !386 && !arm

package all

// Backends that don't support 32bit

import (
	_ "github.com/hidal-go/hidalgo/kv/flat/pebble"
)
