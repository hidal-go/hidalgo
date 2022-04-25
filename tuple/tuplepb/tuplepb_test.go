package tuplepb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/values"
)

func TestTableEncoding(t *testing.T) {
	tbl := &tuple.Header{
		Name: "test",
		Key: []tuple.KeyField{
			{Name: "k1", Type: values.IntType{}, Auto: true},
			{Name: "k2", Type: values.StringType{}},
		},
		Data: []tuple.Field{
			{Name: "f1", Type: values.BytesType{}},
			{Name: "f2", Type: values.StringType{}},
			{Name: "f2", Type: values.FloatType{}},
		},
	}

	data, err := MarshalTable(tbl)
	require.NoError(t, err)

	tbl2, err := UnmarshalTable(data)
	require.NoError(t, err)
	require.Equal(t, tbl, tbl2)
}
