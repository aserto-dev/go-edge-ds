package v3_test

import (
	"os"
	"testing"

	v3 "github.com/aserto-dev/go-edge-ds/pkg/model/v3"
	"github.com/stretchr/testify/require"
)

func TestLoadManifest(t *testing.T) {
	m3 := v3.ModelV3{}

	r, err := os.Open("./manifests/example.yaml")
	require.NoError(t, err)
	require.NotNil(t, r)

	m, err := m3.Read(r)
	require.NoError(t, err)
	require.NotNil(t, m)
}
