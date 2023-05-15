package v3_test

import (
	"os"
	"testing"

	v3 "github.com/aserto-dev/go-edge-ds/pkg/model/v3"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var example = v3.Manifest{
	Name:          "example",
	SchemaVersion: "0.30",
	ObjectTypes: []*v3.ObjectType{
		{
			Name: "object_0",
		},
		{
			Name: "object_1",
			Relations: []*v3.RelationType{
				{
					Name:       "relation_1",
					Definition: "relation_definition_1",
				},
			},
			Permissions: []*v3.PermissionType{
				{
					Name:       "permission_1",
					Definition: "permission_definition_1",
				},
			},
			Annotations: nil,
		},
	},
}

func TestModelV3(t *testing.T) {
	enc := yaml.NewEncoder(os.Stdout)
	enc.SetIndent(2)
	if err := enc.Encode(example); err != nil {
		require.NoError(t, err)
	}
}
