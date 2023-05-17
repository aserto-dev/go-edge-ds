package v2

import (
	"io"

	"github.com/aserto-dev/go-edge-ds/pkg/model"
	"gopkg.in/yaml.v2"
)

const schemaVersion int = 2

// ensure the model interfaces are implemented.
var (
	_ model.Loader = &ModelV2{}
	_ model.Saver  = &ModelV2{}
	_ model.Reader = &ModelV2{}
	_ model.Writer = &ModelV2{}
)

type ModelV2 struct{}

type ObjectTypes map[string]ObjectRelation

type ObjectRelation map[string]Relation

type Relation map[string][]string

func (i *ModelV2) Read(r io.Reader) (*model.Model, error) {
	m2 := ObjectTypes{}
	dec := yaml.NewDecoder(r)
	if err := dec.Decode(&m2); err != nil {
		return nil, err
	}

	m := model.Model{
		Name:          "",
		SchemaVersion: schemaVersion,
		ObjectTypes:   []*model.ObjectType{},
	}

	for objectTypeName, objectRelation := range m2 {
		o := model.ObjectType{
			Name:        objectTypeName,
			Relations:   []*model.RelationType{},
			Permissions: []*model.PermissionType{},
		}

		for relName, rel := range objectRelation {
			r := model.RelationType{
				Name:       relName,
				Definition: "",
				Operation:  model.None,
				Relations:  []string{},
			}

			if v, ok := rel["union"]; ok {
				r.Operation = model.Union
				for i := 0; i < len(v); i++ {
					r.Relations = append(r.Relations, v[i])
				}
			}

			if v, ok := rel["permissions"]; ok {
				r.Operation = model.Union

				for i := 0; i < len(v); i++ {
					p := model.PermissionType{
						Name:        v[i],
						Definition:  "",
						Operation:   model.Union,
						Permissions: []*model.PermissionType{},
					}
					o.Permissions = append(o.Permissions, &p)
				}
			}
		}

		m.ObjectTypes = append(m.ObjectTypes, &o)
	}

	return &m, nil
}

func (i *ModelV2) Write(w io.Writer, m *model.Model) error {
	return nil
}

func (i *ModelV2) Load() (*model.Model, error) {
	return nil, nil
}

func (i *ModelV2) Save(m *model.Model) error {
	return nil
}
