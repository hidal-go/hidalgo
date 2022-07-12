package nosql_test

import (
	"testing"

	"github.com/hidal-go/hidalgo/legacy/nosql"
	"github.com/stretchr/testify/require"
)

var filterMatch = []struct {
	d   nosql.Document
	f   nosql.FieldFilter
	exp bool
}{
	{
		f:   nosql.FieldFilter{Path: []string{"value", "str"}, Filter: nosql.GT, Value: nosql.String("f")},
		d:   nosql.Document{"value": nosql.Document{"str": nosql.String("bob")}},
		exp: false,
	},
	{
		f:   nosql.FieldFilter{Path: []string{"value", "str"}, Filter: nosql.Equal, Value: nosql.String("f")},
		d:   nosql.Document{"value": nosql.Document{"str": nosql.String("bob")}},
		exp: false,
	},
	{
		f:   nosql.FieldFilter{Path: []string{"value", "str"}, Filter: nosql.Equal, Value: nosql.String("bob")},
		d:   nosql.Document{"value": nosql.Document{"str": nosql.String("bob")}},
		exp: true,
	},
	{
		f:   nosql.FieldFilter{Path: []string{"value", "str"}, Filter: nosql.NotEqual, Value: nosql.String("bob")},
		d:   nosql.Document{"value1": nosql.Document{"str": nosql.String("bob")}},
		exp: true,
	},
}

func TestFilterMatch(t *testing.T) {
	for _, c := range filterMatch {
		require.Equal(t, c.exp, c.f.Matches(c.d))
	}
}
