package mig001

import (
	"bytes"
	"time"

	"github.com/Masterminds/semver"
	"github.com/aserto-dev/go-directory/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/metadata"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig"
	"github.com/rs/zerolog"
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

var fnMap = []func(*zerolog.Logger, *bolt.DB, *bolt.DB) error{
	mig.CreateBucket(bdb.SystemPath),
	mig.EnsureBaseVersion,
	mig.CreateBucket(bdb.ObjectTypesPath),
	mig.CreateBucket(bdb.PermissionsPath),
	mig.CreateBucket(bdb.RelationTypesPath),
	mig.CreateBucket(bdb.ObjectsPath),
	mig.CreateBucket(bdb.RelationsSubPath),
	mig.CreateBucket(bdb.RelationsObjPath),
	seed,
}

func Migrate(log *zerolog.Logger, roDB, rwDB *bolt.DB) error {
	log.Info().Str("version", Version).Msg("StartMigration")
	for _, fn := range fnMap {
		if err := fn(log, roDB, rwDB); err != nil {
			return err
		}
	}
	log.Info().Str("version", Version).Msg("FinishedMigration")
	return nil
}

func seed(_ *zerolog.Logger, _, rwDB *bolt.DB) error {
	return rwDB.Update(func(tx *bolt.Tx) error {
		for _, objType := range metadata.ObjectTypes {
			ts := timestamppb.New(time.Now().UTC())
			objType.CreatedAt = ts
			objType.UpdatedAt = ts

			buf := new(bytes.Buffer)
			if err := pb.ProtoToBuf(buf, objType); err != nil {
				return err
			}

			if err := mig.SetKey(tx, bdb.ObjectTypesPath, []byte(objType.Name), buf.Bytes()); err != nil {
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

			if err := mig.SetKey(tx, bdb.RelationTypesPath, []byte(relType.Name), buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
}
