package mig002

import (
	"bytes"
	"fmt"
	"os"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig"
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
	*dsc2.ObjectType | *dsc2.RelationType | *dsc2.Permission
	proto.Message
	GetName() string
}

type objectType interface {
	*dsc2.Object
	proto.Message
	GetType() string
	GetKey() string
	GetProperties() *structpb.Struct
}

type relationType interface {
	*dsc2.Relation
	proto.Message
	GetSubject() *dsc2.ObjectIdentifier
	GetRelation() string
	GetObject() *dsc2.ObjectIdentifier
}

type direction int

const (
	ObjectToSubject direction = iota
	SubjectToObject
)

var fnMap = []func(*bolt.DB, *bolt.DB) error{
	mig.DeleteBucket(bdb.ObjectTypesPath),
	mig.DeleteBucket(ObjectTypesNamePath),
	mig.CreateBucket(bdb.ObjectTypesPath),
	updateModelTypes(bdb.ObjectTypesPath, &dsc2.ObjectType{}),

	mig.DeleteBucket(bdb.RelationTypesPath),
	mig.DeleteBucket(RelationTypesNamePath),
	mig.CreateBucket(bdb.RelationTypesPath),
	updateModelTypes(bdb.RelationTypesPath, &dsc2.RelationType{}),

	mig.DeleteBucket(bdb.PermissionsPath),
	mig.DeleteBucket(PermissionsNamePath),
	mig.CreateBucket(bdb.PermissionsPath),
	updateModelTypes(bdb.PermissionsPath, &dsc2.Permission{}),

	mig.DeleteBucket(bdb.ObjectsPath),
	mig.DeleteBucket(ObjectsKeyPath),
	mig.CreateBucket(bdb.ObjectsPath),
	updateObjects(bdb.ObjectsPath, &dsc2.Object{}),

	mig.DeleteBucket(bdb.RelationsObjPath),
	mig.DeleteBucket(bdb.RelationsSubPath),
	mig.CreateBucket(bdb.RelationsObjPath),
	mig.CreateBucket(bdb.RelationsSubPath),
	updateRelations(bdb.RelationsObjPath, &dsc2.Relation{}, ObjectToSubject),
	updateRelations(bdb.RelationsSubPath, &dsc2.Relation{}, SubjectToObject),
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
func updateModelTypes[T modelType](path bdb.Path, v T) func(*bolt.DB, *bolt.DB) error {
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
	case *dsc2.ObjectType:
		return []byte(msg.GetName())
	case *dsc2.Permission:
		return []byte(msg.GetName())
	case *dsc2.RelationType:
		return []byte(msg.GetObjectType() + ds.TypeIDSeparator + msg.GetName())
	}
	return []byte{}
}

// updateObjects, read values from read-only backup, write to new bucket.
func updateObjects[T objectType](path bdb.Path, v T) func(*bolt.DB, *bolt.DB) error {
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
func updateRelations[T relationType](path bdb.Path, v T, d direction) func(*bolt.DB, *bolt.DB) error {
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
