package v2_test

import (
	"os"
	"testing"

	v2 "github.com/aserto-dev/go-edge-ds/pkg/model/v2"
	"github.com/stretchr/testify/require"
)

func TestLoadManifest(t *testing.T) {
	m2 := v2.ModelV2{}

	r, err := os.Open("./manifests/example.yaml")
	require.NoError(t, err)
	require.NotNil(t, r)

	m, err := m2.Read(r)
	require.NoError(t, err)
	require.NotNil(t, m)
}
