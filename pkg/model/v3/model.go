package v3

type Manifest struct {
	Name          string        `yaml:"name" json:"name"`
	SchemaVersion string        `yaml:"schema_version" json:"schema_version"`
	ObjectTypes   []*ObjectType `yaml:"object_types,omitempty" json:"object_types,omitempty"`
}

type ObjectType struct {
	Name        string                 `yaml:"name" json:"name"`
	Relations   []*RelationType        `yaml:"relations,omitempty" json:"relations,omitempty"`
	Permissions []*PermissionType      `yaml:"permissions,omitempty" json:"permissions,omitempty"`
	Annotations map[string]interface{} `yaml:"annotations,omitempty" json:"annotations,omitempty"`
}

type RelationType struct {
	Name       string `yaml:"name" json:"name"`
	Definition string `yaml:"definition" json:"definition"`
}

type PermissionType struct {
	Name       string `yaml:"name" json:"name"`
	Definition string `yaml:"definition" json:"definition"`
}
