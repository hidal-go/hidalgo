package tuple

import (
	"context"
	"errors"
)

// ErrWildGuess returned if the the size can only be randomly guessed by the backend without scanning the data.
var ErrWildGuess = errors.New("can only guess the size")

// TableSize returns a number of records in a table matching the filter.
// If exact is set to false, an estimate will be returned.
// If estimate cannot be obtained without scanning the whole table, ErrWildGuess will be returned
// with some random number.
func TableSize(ctx context.Context, t Table, f *Filter, exact bool) (int64, error) {
	// TODO: optimize for backends that can provide this functionality
	if !exact {
		return 1000, ErrWildGuess
	}
	it := t.Scan(&ScanOptions{
		KeysOnly: true,
		Filter:   f,
	})

	var n int64
	for it.Next(ctx) {
		n++
	}

	return n, it.Err()
}
