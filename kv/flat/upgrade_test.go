package flat

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hidal-go/hidalgo/kv"
)

func TestSepEscape(t *testing.T) {
	k := kv.Key{
		[]byte(`\/aa/b\b/c/d/\`),
		[]byte(`/aa/b\b/c/d/`),
	}
	k2 := KeyUnescape(KeyEscape(k))
	require.Equal(t, k, k2)
}
