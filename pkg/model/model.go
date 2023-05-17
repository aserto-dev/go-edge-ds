package model

import "io"

type Operation int

const (
	None         Operation = iota // NONE
	Union                         // OR
	Intersection                  // AND
	Exclusion                     // NOT (slot 0 = base, slot 1 = subtraction)
)

type Model struct {
	Name          string        `yaml:"name" json:"name"`
	SchemaVersion int           `yaml:"schema_version" json:"schema_version"`
	ObjectTypes   []*ObjectType `yaml:"object_types,omitempty" json:"object_types,omitempty"`
}

type ObjectType struct {
	Name        string            `yaml:"name" json:"name"`
	Relations   []*RelationType   `yaml:"relations,omitempty" json:"relations,omitempty"`
	Permissions []*PermissionType `yaml:"permissions,omitempty" json:"permissions,omitempty"`
}

type RelationType struct {
	Name       string    `yaml:"name" json:"name"`
	Definition string    `yaml:"definition" json:"definition"`
	Operation  Operation `yaml:"operation" json:"operation"`
	Relations  []string  `yaml:"relations,omitempty" json:"relations,omitempty"`
}

type PermissionType struct {
	Name        string    `yaml:"name" json:"name"`
	Definition  string    `yaml:"definition" json:"definition"`
	Operation   Operation `yaml:"operation" json:"operation"`
	Permissions []string  `yaml:"permissions,omitempty" json:"permissions,omitempty"`
}

func (m *Model) ObjectTypeExists(objectType string) bool {
	return false
}

func (m *Model) RelationExists(objectType, relation string) bool {
	return false
}

func (m *Model) PermissionExists(objectType, permission string) bool {
	return false
}

func (m *Model) ResolveRelation(objectType, relation string) ([]string, error) {
	return []string{}, nil
}

func (m *Model) ResolvePermission(objectType, permission string) ([]string, error) {
	return []string{}, nil
}

// Read model instance from manifest.
type Reader interface {
	Read(io.Reader) (*Model, error)
}

// Write model instance to manifest.
type Writer interface {
	Write(io.Writer, *Model) error
}

// Load model instance from directory store.
type Loader interface {
	Load() (*Model, error)
}

// Save model instance to directory store.
type Saver interface {
	Save(*Model) error
}

type Object struct {
	Relations map[string]Relation
}

type Relation struct {
	Union       []string
	Permissions []string
}
