package bdb

type Path []string

var (
	SystemPath        Path = []string{"_system"}
	ManifestPath      Path = []string{"_manifest", ManifestName, ManifestVersion}
	ObjectTypesPath   Path = []string{"object_types"}
	PermissionsPath   Path = []string{"permissions"}
	RelationTypesPath Path = []string{"relation_types"}
	ObjectsPath       Path = []string{"objects"}
	RelationsSubPath  Path = []string{"relations_sub"}
	RelationsObjPath  Path = []string{"relations_obj"}
	MetadataKey            = []byte("metadata")
	BodyKey                = []byte("body")
	ModelKey               = []byte("model")
)

const (
	ManifestName    string = "default"
	ManifestVersion string = "0.0.1"
)
