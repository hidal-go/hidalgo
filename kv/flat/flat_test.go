package flat

import (
	"testing"

	"github.com/hidal-go/hidalgo/kv"
	"github.com/stretchr/testify/require"
)

func TestSepEscape(t *testing.T) {
	k := kv.Key{
		[]byte(`\/aa/b\b/c/d/\`),
		[]byte(`/aa/b\b/c/d/`),
	}
	k2 := keyUnescape(keyEscape(k))
	require.Equal(t, k, k2)
}
