package tuplepb

import (
	"testing"

	"github.com/hidal-go/hidalgo/tuple"
	"github.com/hidal-go/hidalgo/types"
	"github.com/stretchr/testify/require"
)

func TestTableEncoding(t *testing.T) {
	tbl := &tuple.Header{
		Name: "test",
		Key: []tuple.KeyField{
			{Name: "k1", Type: types.IntType{}, Auto: true},
			{Name: "k2", Type: types.StringType{}},
		},
		Data: []tuple.Field{
			{Name: "f1", Type: types.BytesType{}},
			{Name: "f2", Type: types.StringType{}},
			{Name: "f2", Type: types.FloatType{}},
		},
	}

	data, err := MarshalTable(tbl)
	require.NoError(t, err)

	tbl2, err := UnmarshalTable(data)
	require.NoError(t, err)
	require.Equal(t, tbl, tbl2)
}
