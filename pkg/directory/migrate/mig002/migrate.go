package mig002

import (
	"bytes"
	"fmt"
	"os"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/directory/migrate/mig"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	Version string = "0.0.2"
)

var (
	ObjectTypesNamePath   = []string{"object_types_name"}
	RelationTypesNamePath = []string{"relation_types_name"}
	PermissionsNamePath   = []string{"permissions_name"}
	ObjectsKeyPath        = []string{"objects_key"}
)

func MigrationVersion() *semver.Version {
	migVersion, _ := semver.NewVersion(Version)
	return migVersion
}

type modelType interface {
	*dsc.ObjectType | *dsc.RelationType | *dsc.Permission
	proto.Message
	GetName() string
}

type objectType interface {
	*dsc.Object
	proto.Message
	GetType() string
	GetKey() string
	GetProperties() *structpb.Struct
}

type relationType interface {
	*dsc.Relation
	proto.Message
	GetSubject() *dsc.ObjectIdentifier
	GetRelation() string
	GetObject() *dsc.ObjectIdentifier
}

type direction int

const (
	ObjectToSubject direction = iota
	SubjectToObject
)

var fnMap = []func(*bolt.DB, *bolt.DB) error{
	mig.DeleteBucket(boltdb.ObjectTypesPath),
	mig.DeleteBucket(ObjectTypesNamePath),
	mig.CreateBucket(boltdb.ObjectTypesPath),
	updateModelTypes(boltdb.ObjectTypesPath, &dsc.ObjectType{}),

	mig.DeleteBucket(boltdb.RelationTypesPath),
	mig.DeleteBucket(RelationTypesNamePath),
	mig.CreateBucket(boltdb.RelationTypesPath),
	updateModelTypes(boltdb.RelationTypesPath, &dsc.RelationType{}),

	mig.DeleteBucket(boltdb.PermissionsPath),
	mig.DeleteBucket(PermissionsNamePath),
	mig.CreateBucket(boltdb.PermissionsPath),
	updateModelTypes(boltdb.PermissionsPath, &dsc.Permission{}),

	mig.DeleteBucket(boltdb.ObjectsPath),
	mig.DeleteBucket(ObjectsKeyPath),
	mig.CreateBucket(boltdb.ObjectsPath),
	updateObjects(boltdb.ObjectsPath, &dsc.Object{}),

	mig.DeleteBucket(boltdb.RelationsObjPath),
	mig.DeleteBucket(boltdb.RelationsSubPath),
	mig.CreateBucket(boltdb.RelationsObjPath),
	mig.CreateBucket(boltdb.RelationsSubPath),
	updateRelations(boltdb.RelationsObjPath, &dsc.Relation{}, ObjectToSubject),
	updateRelations(boltdb.RelationsSubPath, &dsc.Relation{}, SubjectToObject),
}

func Migrate(roDB, rwDB *bolt.DB) error {
	for _, fn := range fnMap {
		if err := fn(roDB, rwDB); err != nil {
			return err
		}
	}
	return nil
}

// updateModelTypes, read values from read-only backup, write to new bucket.
func updateModelTypes[T modelType](path []string, v T) func(*bolt.DB, *bolt.DB) error {
	return func(roDB *bolt.DB, rwDB *bolt.DB) error {

		if err := roDB.View(func(tx *bolt.Tx) error {
			b, err := mig.SetBucket(tx, path)
			if err != nil {
				return err
			}

			c := b.Cursor()
			for key, value := c.First(); key != nil; key, value = c.Next() {
				if key == nil {
					break
				}

				if err := pb.BufToProto(bytes.NewReader(value), any(v).(proto.Message)); err != nil {
					return err
				}

				buf := new(bytes.Buffer)
				if err := pb.ProtoToBuf(buf, v); err != nil {
					return err
				}

				if err := rwDB.Update(func(tx *bolt.Tx) error {
					return mig.SetKey(tx, path, keyModelType(v), buf.Bytes())
				}); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}

		return nil
	}
}

func keyModelType[T modelType](v T) []byte {
	var i interface{} = v
	switch msg := i.(type) {
	case *dsc.ObjectType:
		return []byte(msg.GetName())
	case *dsc.Permission:
		return []byte(msg.GetName())
	case *dsc.RelationType:
		return []byte(msg.GetObjectType() + ds.TypeIDSeparator + msg.GetName())
	}
	return []byte{}
}

// updateObjects, read values from read-only backup, write to new bucket.
func updateObjects[T objectType](path []string, v T) func(*bolt.DB, *bolt.DB) error {
	return func(roDB *bolt.DB, rwDB *bolt.DB) error {

		if err := roDB.View(func(tx *bolt.Tx) error {
			b, err := mig.SetBucket(tx, path)
			if err != nil {
				return err
			}

			c := b.Cursor()
			for key, value := c.First(); key != nil; key, value = c.Next() {
				if key == nil {
					break
				}

				if err := pb.BufToProto(bytes.NewReader(value), any(v).(proto.Message)); err != nil {
					return err
				}

				buf := new(bytes.Buffer)
				if err := pb.ProtoToBuf(buf, v); err != nil {
					return err
				}

				newKey := v.GetType() + ds.TypeIDSeparator + v.GetKey()

				if err := rwDB.Update(func(tx *bolt.Tx) error {
					return mig.SetKey(tx, path, []byte(newKey), buf.Bytes())
				}); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}

		return nil
	}
}

// updateRelations, read values from read-only backup, write to new bucket.
func updateRelations[T relationType](path []string, v T, d direction) func(*bolt.DB, *bolt.DB) error {
	return func(roDB *bolt.DB, rwDB *bolt.DB) error {

		if err := roDB.View(func(tx *bolt.Tx) error {
			b, err := mig.SetBucket(tx, path)
			if err != nil {
				return err
			}

			c := b.Cursor()
			for key, value := c.First(); key != nil; key, value = c.Next() {
				if key == nil {
					break
				}

				if err := pb.BufToProto(bytes.NewReader(value), any(v).(proto.Message)); err != nil {
					return err
				}

				buf := new(bytes.Buffer)
				if err := pb.ProtoToBuf(buf, v); err != nil {
					return err
				}

				if v.GetObject().GetKey() == "" || v.GetSubject().GetKey() == "" {
					return errors.Wrapf(os.ErrInvalid, "relation does not contain key values")
				}

				newKey := relKey(v, d)

				if err := rwDB.Update(func(tx *bolt.Tx) error {
					return mig.SetKey(tx, path, []byte(newKey), buf.Bytes())
				}); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}

		return nil
	}
}

// relKey, generates the new relation key using the object type and key instead of object id.
func relKey[T relationType](v T, d direction) string {
	switch d {
	// obj_type : obj_id | relation | sub_type : sub_id
	// when subject_relation is added this will become
	// obj_type : obj_id | relation | sub_type : sub_id (# sub_rel)
	case ObjectToSubject:
		return fmt.Sprintf("%s:%s|%s|%s:%s",
			v.GetObject().GetType(),
			v.GetObject().GetKey(),
			v.GetRelation(),
			v.GetSubject().GetType(),
			v.GetSubject().GetKey(),
		)
	// sub_type : sub_id | relation | obj_type : obj_id
	// when subject_relation is added this will become
	// sub_type : sub_id (# sub_rel) | relation | obj_type : obj_id
	case SubjectToObject:
		return fmt.Sprintf("%s:%s|%s|%s:%s",
			v.GetSubject().GetType(),
			v.GetSubject().GetKey(),
			v.GetRelation(),
			v.GetObject().GetType(),
			v.GetObject().GetKey(),
		)
	default:
		return ""
	}
}
