package flat_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/hidal-go/hidalgo/kv/flat"
)

func TestSepEscape(t *testing.T) {
	k := kv.Key{
		[]byte(`\/aa/b\b/c/d/\`),
		[]byte(`/aa/b\b/c/d/`),
	}
	k2 := flat.KeyUnescape(flat.KeyEscape(k))
	require.Equal(t, k, k2)
}
