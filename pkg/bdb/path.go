package bdb

type Path []string

var (
	SystemPath        Path = []string{"_system"}
	ManifestPath      Path = []string{"_manifest"}
	ObjectTypesPath   Path = []string{"object_types"}
	PermissionsPath   Path = []string{"permissions"}
	RelationTypesPath Path = []string{"relation_types"}
	ObjectsPath       Path = []string{"objects"}
	RelationsSubPath  Path = []string{"relations_sub"}
	RelationsObjPath  Path = []string{"relations_obj"}
)
