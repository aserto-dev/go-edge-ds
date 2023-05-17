package v3

import (
	"errors"
	"fmt"
	"io"

	"github.com/aserto-dev/go-edge-ds/pkg/model"
	"gopkg.in/yaml.v3"
)

const SchemaVersion int = 3

var (
	ErrSchemaVersion       = errors.New("invalid schema version")
	ErrObjectTypeUnknown   = errors.New("object type unknown")
	ErrRelationTypeUnknown = errors.New("relation type unknown")
	ErrPermissionUnknown   = errors.New("permission unknown")
)

// ensure the model interfaces are implemented.
var (
	_ model.Reader = &ModelV3{}
	_ model.Writer = &ModelV3{}
	_ model.Loader = &ModelV3{}
	_ model.Saver  = &ModelV3{}
)

type ModelV3 struct{}

type Manifest struct {
	Info        Info                  `yaml:"model,omitempty" json:"model,omitempty"`
	ObjectTypes map[string]ObjectType `yaml:"object_types,omitempty" json:"object_types,omitempty"`
}

type Info struct {
	SchemaVersion int `yaml:"schema_version,omitempty" json:"schema_version,omitempty"`
}

type ObjectType struct {
	Name        string                 `yaml:"-" json:"name,omitempty"`
	Relations   map[string]string      `yaml:"relations,omitempty" json:"relations,omitempty"`
	Permissions map[string]string      `yaml:"permissions,omitempty" json:"permissions,omitempty"`
	Properties  map[string]interface{} `yaml:"properties,omitempty" json:"properties,omitempty"`
	// relations   []*RelationType
	// permissions []*PermissionType
}

type RelationType struct {
	Name       string `yaml:"name" json:"name"`
	Definition string `yaml:"definition" json:"definition"`
}

type PermissionType struct {
	Name       string `yaml:"name" json:"name"`
	Definition string `yaml:"definition" json:"definition"`
}

type manifest Manifest

func (m *Manifest) UnmarshalYAML(value *yaml.Node) error {
	var raw manifest

	if err := value.Decode(&raw); err != nil {
		return err
	}

	for k, v := range raw.ObjectTypes {
		v.Name = k
		raw.ObjectTypes[k] = v
	}

	*m = Manifest(raw)

	return nil
}

type info Info

func (m *Info) UnmarshalYAML(value *yaml.Node) error {
	var raw info
	if err := value.Decode(&raw); err != nil {
		return err
	}

	if raw.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w %d", ErrSchemaVersion, raw.SchemaVersion)
	}

	*m = Info(raw)

	return nil
}

type objectType ObjectType

func (m *ObjectType) UnmarshalYAML(value *yaml.Node) error {
	var raw objectType
	if err := value.Decode(&raw); err != nil {
		return err
	}

	*m = ObjectType(raw)

	return nil
}

func (i *ModelV3) Read(r io.Reader) (*model.Model, error) {
	manifest := Manifest{}
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&manifest); err != nil {
		return nil, err
	}

	return nil, nil
}

func (i *ModelV3) Write(w io.Writer, m *model.Model) error {
	return nil
}

func (i *ModelV3) Load() (*model.Model, error) {
	return nil, nil
}

func (i *ModelV3) Save(m *model.Model) error {
	return nil
}
