package model_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/aserto-dev/go-edge-ds/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestParse(t *testing.T) {
	var re = regexp.MustCompile(`(?m)^(?P<object_type>[^#]+)(?:#(?P<relation_type>([^:]+)))?(?::(?P<permission>(.+)))?$`)
	var str = []string{
		"my_object_type",
		"my_object_type#my_relation_type",
		"my_object_type#my_relation_type:my_permission",
	}

	for _, s := range str {
		match := re.FindStringSubmatch(s)
		fmt.Println("object_type", match[re.SubexpIndex("object_type")])
		fmt.Println("relation_type", match[re.SubexpIndex("relation_type")])
		fmt.Println("permission", match[re.SubexpIndex("permission")])
	}
}

func TestLoadManifest(t *testing.T) {
	dir := "./manifest"
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		t.Run(file.Name(), func(t *testing.T) {
			t.Logf("%s", filepath.Join(dir, file.Name()))
			m, err := model.Open(filepath.Join(dir, file.Name()))
			assert.NoError(t, err)
			assert.NotNil(t, m)
		})
	}
}

func TestLoadManifest1(t *testing.T) {
	dir := "./manifest"
	file := "sample.yaml"
	t.Logf("%s", filepath.Join(dir, file))
	m, err := model.Open(filepath.Join(dir, file))
	assert.NoError(t, err)
	assert.NotNil(t, m)
}

type Manifest struct {
	Model       *Model                 `yaml:"model,omitempty" json:"model,omitempty"`
	ObjectTypes map[string]*ObjectType `yaml:"object_types,omitempty" json:"object_types,omitempty"`
}

type Model struct {
	SchemaVersion int `yaml:"schema_version,omitempty" json:"schema_version,omitempty"`
}

type ObjectType struct {
	Name      string               `yaml:"-" json:"-"`
	Relations map[string]*Relation `yaml:"relations,omitempty" json:"relations,omitempty"`
}

type Relation struct {
	Name        string   `yaml:"-" json:"-"`
	Unions      []string `yaml:"union,omitempty" json:"union,omitempty"`
	Permissions []string `yaml:"permissions,omitempty" json:"permissions,omitempty"`
}

var defaultManifest = Manifest{
	Model: &Model{
		SchemaVersion: 1,
	},
	ObjectTypes: map[string]*ObjectType{
		"user": {
			Name: "user",
			Relations: map[string]*Relation{
				"manager": {
					Name: "manager",
				},
			},
		},
		"identity": {
			Name: "identity",
			Relations: map[string]*Relation{
				"identifier": nil,
			},
		},
		"group": {
			Name: "group",
			Relations: map[string]*Relation{
				"member": nil,
			},
		},
		"system": {
			Name: "system",
			Relations: map[string]*Relation{
				"user": nil,
			},
		},
		"application": {
			Name: "application",
			Relations: map[string]*Relation{
				"user": nil,
			},
		},
		"resource": nil,
		"user-v1":  nil,
	},
}

var sampleManifest = Manifest{
	Model: &Model{
		SchemaVersion: 1,
	},
	ObjectTypes: map[string]*ObjectType{
		"document": {
			Name: "document",
			Relations: map[string]*Relation{
				"owner": {
					Name: "owner",
					Unions: []string{
						"editor",
					},
					Permissions: []string{
						"sample.document.delete",
					},
				},
				"editor": {
					Name: "editor",
					Unions: []string{
						"viewer",
					},
					Permissions: []string{
						"sample.document.create",
						"sample.document.update",
					},
				},
				"viewer": {
					Name: "viewer",
					Permissions: []string{
						"sample.document.read",
					},
				},
			},
		},
	},
}

var rai2OptManifest = Manifest{
	Model: &Model{
		SchemaVersion: 1,
	},
	ObjectTypes: map[string]*ObjectType{
		"user": nil,
		"identity": {
			Relations: map[string]*Relation{
				"identifier": nil,
			},
		},
		"account": {
			Relations: map[string]*Relation{
				"member": {},
				"owner": {
					Unions: []string{
						"contributor",
					},
					Permissions: []string{
						"delete",
					},
				},
				"contributor": {
					Unions: []string{
						"reader",
					},
					Permissions: []string{
						"update",
						"create_write_transaction",
					},
				},
				"reader": {
					Permissions: []string{
						"read",
						"create_read_transaction",
						"list_transactions",
					},
				},
			},
		},
		"resource_group": {
			Relations: map[string]*Relation{
				"parent": {},
				"owner": {
					Unions: []string{
						"contributor",
					},
					Permissions: []string{
						"delete",
					},
				},
				"contributor": {
					Unions: []string{
						"reader",
					},
					Permissions: []string{
						"update",
					},
				},
				"reader": {
					Permissions: []string{
						"read",
					},
				},
			},
		},
		"engine": {
			Relations: map[string]*Relation{
				"parent": {},
				"owner": {
					Unions: []string{
						"contributor",
					},
					Permissions: []string{
						"delete",
					},
				},
				"contributor": {
					Unions: []string{
						"reader",
					},
					Permissions: []string{
						"update",
					},
				},
				"reader": {
					Permissions: []string{
						"read",
					},
				},
			},
		},
		"database": {
			Relations: map[string]*Relation{
				"parent": {},
				"owner": {
					Unions: []string{
						"contributor",
					},
					Permissions: []string{
						"delete",
					},
				},
				"contributor": {
					Unions: []string{
						"reader",
					},
					Permissions: []string{
						"update",
						"create_write_transaction",
					},
				},
				"reader": {
					Permissions: []string{
						"read",
						"create_read_transaction",
						"list_transactions",
					},
				},
			},
		},
	},
}

func TestSerialize(t *testing.T) {
	var manifests = []*Manifest{
		&defaultManifest,
		&sampleManifest,
		&rai2OptManifest,
	}
	for _, m := range manifests {
		{
			dec := yaml.NewEncoder(os.Stdout)
			if err := dec.Encode(m); err != nil {
				require.NoError(t, err)
			}
		}
		{
			dec := json.NewEncoder(os.Stdout)
			dec.SetIndent("", "  ")
			if err := dec.Encode(m); err != nil {
				require.NoError(t, err)
			}
		}
	}
}

func Open(filename string) (*Manifest, error) {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	manifest := Manifest{}
	if err := yaml.Unmarshal(buf, &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func TestLoadManifest2(t *testing.T) {
	dir := "./manifest"
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		t.Run(file.Name(), func(t *testing.T) {
			t.Logf("%s", filepath.Join(dir, file.Name()))
			m, err := Open(filepath.Join(dir, file.Name()))
			assert.NoError(t, err)
			assert.NotNil(t, m)
		})
	}
}
