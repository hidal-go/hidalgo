package couch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelector(t *testing.T) {
	q := ouchQuery{"selector": make(map[string]interface{})}
	in := []interface{}{"a", "b"}
	q.putSelector(idField, map[string]interface{}{"$in": in})
	q.putSelector(idField, map[string]interface{}{"$gt": "a"})
	require.Equal(t, ouchQuery{
		"selector": map[string]interface{}{
			idField: map[string]interface{}{
				"$in": in,
				"$gt": "a",
			},
		},
	}, q)
}
