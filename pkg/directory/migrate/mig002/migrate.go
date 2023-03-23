package mig002

import (
	"bytes"
	"fmt"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

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

var fnMap = []func(*boltdb.BoltDB) error{
	switchSchemaIDsToName(types.ObjectTypesPath(), &dsc.ObjectType{}),
	deleteBucket(types.ObjectTypesNamePath()),

	switchSchemaIDsToName(types.RelationTypesPath(), &dsc.RelationType{}),
	deleteBucket(types.RelationTypesNamePath()),

	switchSchemaIDsToName(types.PermissionsPath(), &dsc.Permission{}),
	deleteBucket(types.PermissionsNamePath()),

	switchObjectIDs(types.ObjectsPath(), &dsc.Object{}),
	deleteBucket(types.ObjectsKeyPath()),

	updateRelations(types.RelationsObjPath(), &dsc.Relation{}, ObjectToSubject),
	updateRelations(types.RelationsSubPath(), &dsc.Relation{}, SubjectToObject),
}

func Migrate(store *boltdb.BoltDB) error {
	for _, fn := range fnMap {
		if err := fn(store); err != nil {
			return err
		}
	}

	return nil
}

// switchSchemaIDsToName
// steps:
// - created instance in new struct layout.
// - delete existing id-based entry.
// - write new name-based entry.
func switchSchemaIDsToName[T modelType](path []string, v T) func(*boltdb.BoltDB) error {
	return func(store *boltdb.BoltDB) error {
		txOpt, cleanup, err := store.WriteTxOpts()
		if err != nil {
			return err
		}
		defer func() {
			cErr := cleanup(err)
			if cErr != nil {
				err = cErr
			}
		}()

		pageToken := ""
		pageSize := int32(100)
		opts := []boltdb.Opts{txOpt}

		for {
			keys, values, nextToken, _, err := store.List(path, pageToken, pageSize, opts)
			if err != nil {
				return err
			}

			for i := 0; i < len(keys); i++ {
				// create instance of new struct layout
				if err := pb.BufToProto(bytes.NewReader(values[i]), any(v).(proto.Message)); err != nil {
					return err
				}

				// delete existing entry which is using id as the key.
				if err := store.DeleteKey(path, keys[i], opts); err != nil {
					return err
				}

				// write new entry using name as the key, using the newly marshaled value
				buf := new(bytes.Buffer)
				if err := pb.ProtoToBuf(buf, v); err != nil {
					return err
				}

				if err := store.Write(path, v.GetName(), buf.Bytes(), opts); err != nil {
					return err
				}
			}

			if nextToken == "" {
				break
			}
		}

		return nil
	}
}

func deleteBucket(path []string) func(*boltdb.BoltDB) error {
	return func(store *boltdb.BoltDB) error {
		txOpt, cleanup, err := store.WriteTxOpts()
		if err != nil {
			return err
		}
		defer func() {
			cErr := cleanup(err)
			if cErr != nil {
				err = cErr
			}
		}()

		if err := store.DeleteBucket(path, []boltdb.Opts{txOpt}); err != nil {
			return err
		}

		return nil
	}
}

func switchObjectIDs[T objectType](path []string, v T) func(*boltdb.BoltDB) error {
	return func(store *boltdb.BoltDB) error {
		txOpt, cleanup, err := store.WriteTxOpts()
		if err != nil {
			return err
		}
		defer func() {
			cErr := cleanup(err)
			if cErr != nil {
				err = cErr
			}
		}()

		pageToken := ""
		pageSize := int32(100)
		opts := []boltdb.Opts{txOpt}

		for {
			keys, values, nextToken, _, err := store.List(path, pageToken, pageSize, opts)
			if err != nil {
				return err
			}

			for i := 0; i < len(keys); i++ {
				// create instance of new struct layout
				if err := pb.BufToProto(bytes.NewReader(values[i]), any(v).(proto.Message)); err != nil {
					return err
				}

				// delete existing entry which is using id as the key.
				if err := store.DeleteKey(path, keys[i], opts); err != nil {
					return err
				}

				// write new entry using name as the key, using the newly marshaled value
				buf := new(bytes.Buffer)
				if err := pb.ProtoToBuf(buf, v); err != nil {
					return err
				}

				key := v.GetType() + ":" + v.GetKey()
				if err := store.Write(path, key, buf.Bytes(), opts); err != nil {
					return err
				}
			}

			if nextToken == "" {
				break
			}
		}

		return nil
	}
}

func updateRelations[T relationType](path []string, v T, d direction) func(*boltdb.BoltDB) error {
	return func(store *boltdb.BoltDB) error {
		txOpt, cleanup, err := store.WriteTxOpts()
		if err != nil {
			return err
		}
		defer func() {
			cErr := cleanup(err)
			if cErr != nil {
				err = cErr
			}
		}()

		pageToken := ""
		pageSize := int32(100)
		opts := []boltdb.Opts{txOpt}

		for {
			keys, values, nextToken, _, err := store.List(path, pageToken, pageSize, opts)
			if err != nil {
				return err
			}

			for i := 0; i < len(keys); i++ {
				// create instance of new struct layout
				if err := pb.BufToProto(bytes.NewReader(values[i]), any(v).(proto.Message)); err != nil {
					return err
				}

				// delete existing entry which is using id as the key.
				if err := store.DeleteKey(path, keys[i], opts); err != nil {
					return err
				}

				// write new entry using name as the key, using the newly marshaled value
				buf := new(bytes.Buffer)
				if err := pb.ProtoToBuf(buf, v); err != nil {
					return err
				}

				if err := store.Write(path, relKey(v, d), buf.Bytes(), opts); err != nil {
					return err
				}
			}

			if nextToken == "" {
				break
			}
		}

		return nil
	}
}

func relKey[T relationType](v T, d direction) string {
	switch d {
	// obj_type : obj_id # relation @ sub_type : sub_id
	case ObjectToSubject:
		return fmt.Sprintf("%s:%s#%s@%s:%s",
			v.GetObject().GetType(),
			v.GetObject().GetKey(),
			v.GetRelation(),
			v.GetSubject().GetType(),
			v.GetSubject().GetKey(),
		)
	// sub_type : sub_id @ relation # obj_type : obj_id
	case SubjectToObject:
		return fmt.Sprintf("%s:%s@%s#%s:%s",
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
