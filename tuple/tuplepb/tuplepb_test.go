package tuplepb

import (
	"testing"

	"github.com/nwca/uda/tuple"
	"github.com/nwca/uda/types"
	"github.com/stretchr/testify/require"
)

func TestTableEncoding(t *testing.T) {
	var (
		typBin types.Bytes
		typInt types.String
		typStr types.String
		typFlt types.Float
	)
	tbl := &tuple.Header{
		Name: "test",
		Key: []tuple.KeyField{
			{Name: "k1", Type: &typInt, Auto: true},
			{Name: "k2", Type: &typStr},
		},
		Data: []tuple.Field{
			{Name: "f1", Type: &typBin},
			{Name: "f2", Type: &typStr},
			{Name: "f2", Type: &typFlt},
		},
	}

	data, err := MarshalTable(tbl)
	require.NoError(t, err)

	tbl2, err := UnmarshalTable(data)
	require.NoError(t, err)
	require.Equal(t, tbl, tbl2)
}
