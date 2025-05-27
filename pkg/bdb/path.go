package bdb

type Path []string

var (
	SystemPath        Path = []string{"_system"}
	ManifestPath      Path = []string{"_manifest", ManifestName, ManifestVersion}
	ManifestPathV2    Path = []string{"_manifest", ManifestName}
	ObjectTypesPath   Path = []string{"object_types"}
	PermissionsPath   Path = []string{"permissions"}
	RelationTypesPath Path = []string{"relation_types"}
	ObjectsPath       Path = []string{"objects"}
	RelationsSubPath  Path = []string{"relations_sub"}
	RelationsObjPath  Path = []string{"relations_obj"}
	ManifestKey            = []byte("manifest") // _manifest/default:manifest
	ModelKey               = []byte("model")    // _manifest/default:model
	BodyKey                = []byte("body")     // obsolete
	MetadataKey            = []byte("metadata") // obsolete
)

const (
	ManifestName    string = "default"
	ManifestVersion string = "0.0.1"
)
