package mig001

import (
	"bytes"
	"time"

	"github.com/Masterminds/semver"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/directory/metadata"
	"github.com/aserto-dev/go-edge-ds/pkg/directory/migrate/mig"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	Version string = "0.0.1"
)

func MigrationVersion() *semver.Version {
	migVersion, _ := semver.NewVersion(Version)
	return migVersion
}

var fnMap = []func(*bolt.DB, *bolt.DB) error{
	mig.CreateBucket(boltdb.SystemPath),
	mig.EnsureBaseVersion,
	mig.CreateBucket(boltdb.ObjectTypesPath),
	mig.CreateBucket(boltdb.PermissionsPath),
	mig.CreateBucket(boltdb.RelationTypesPath),
	mig.CreateBucket(boltdb.ObjectsPath),
	mig.CreateBucket(boltdb.RelationsSubPath),
	mig.CreateBucket(boltdb.RelationsObjPath),
	seed,
}

func Migrate(roDB, rwDB *bolt.DB) error {
	for _, fn := range fnMap {
		if err := fn(roDB, rwDB); err != nil {
			return err
		}
	}
	return nil
}

func seed(_, rwDB *bolt.DB) error {
	return rwDB.Update(func(tx *bolt.Tx) error {
		for _, objType := range metadata.ObjectTypes {
			ts := timestamppb.New(time.Now().UTC())
			objType.CreatedAt = ts
			objType.UpdatedAt = ts

			buf := new(bytes.Buffer)
			if err := pb.ProtoToBuf(buf, objType); err != nil {
				return err
			}

			if err := mig.SetKey(tx, boltdb.ObjectTypesPath, []byte(objType.Name), buf.Bytes()); err != nil {
				return err
			}
		}

		for _, relType := range metadata.RelationTypes {
			ts := timestamppb.New(time.Now().UTC())
			relType.CreatedAt = ts
			relType.UpdatedAt = ts

			buf := new(bytes.Buffer)
			if err := pb.ProtoToBuf(buf, relType); err != nil {
				return err
			}

			if err := mig.SetKey(tx, boltdb.RelationTypesPath, []byte(relType.Name), buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
}
