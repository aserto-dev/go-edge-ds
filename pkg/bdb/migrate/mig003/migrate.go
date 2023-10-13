package mig003

import (
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig"
	"github.com/aserto-dev/go-edge-ds/pkg/convert"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/rs/zerolog"

	bolt "go.etcd.io/bbolt"
)

// mig003
//
// backup current database file
// mount current database file as read-only
// add _manifest bucket
// convert object_types, relation_types, permissions to annotated v3 manifest
// set manifest
// set model
// copy object (with schema check)
// copy relations (with schema check)
// set schema to 3

const (
	Version string = "0.0.3"
)

var fnMap = []func(*zerolog.Logger, *bolt.DB, *bolt.DB) error{
	mig.CreateBucket(bdb.ManifestPath),
	migrateModel(),

	mig.DeleteBucket(bdb.ObjectTypesPath),
	mig.DeleteBucket(bdb.RelationTypesPath),
	mig.DeleteBucket(bdb.PermissionsPath),

	mig.DeleteBucket(bdb.ObjectsPath),
	mig.CreateBucket(bdb.ObjectsPath),
	updateObjects(bdb.ObjectsPath),

	mig.DeleteBucket(bdb.RelationsObjPath),
	mig.CreateBucket(bdb.RelationsObjPath),
	updateRelations(bdb.RelationsObjPath, ds.ObjectToSubject),

	mig.DeleteBucket(bdb.RelationsSubPath),
	mig.CreateBucket(bdb.RelationsSubPath),
	updateRelations(bdb.RelationsSubPath, ds.SubjectToObject),
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

// migrateModel,
// 1) creates a manifest file from the metadata objects in the db
// 2) computes the in-memory model
// 2) perists the manifest file byte-stream in the store
// 3) perists the serialized model in the store
func migrateModel() func(*zerolog.Logger, *bolt.DB, *bolt.DB) error {
	return func(log *zerolog.Logger, roDB *bolt.DB, rwDB *bolt.DB) error {
		log.Info().Str("version", Version).Msg("MigrateModel")

		return nil
	}
}

// updateObjects, read values from read-only backup, write to new bucket.
func updateObjects(path bdb.Path) func(*zerolog.Logger, *bolt.DB, *bolt.DB) error {
	return func(log *zerolog.Logger, roDB *bolt.DB, rwDB *bolt.DB) error {
		log.Info().Str("version", Version).Msg("UpdateObjects")

		if err := roDB.View(func(rtx *bolt.Tx) error {
			wtx, err := rwDB.Begin(true)
			if err != nil {
				return err
			}
			defer wtx.Rollback()

			b, err := mig.SetBucket(rtx, path)
			if err != nil {
				return err
			}

			c := b.Cursor()
			for key, value := c.First(); key != nil; key, value = c.Next() {
				if key == nil {
					break
				}

				o2, err := bdb.Unmarshal[dsc2.Object](value)
				if err != nil {
					return err
				}

				o3 := convert.ObjectToV3(o2)

				b3, err := bdb.Marshal[dsc3.Object](o3)
				if err != nil {
					return err
				}

				newKey := ds.Object(o3).Key()

				if err := mig.SetKey(wtx, path, []byte(newKey), b3); err != nil {
					return err
				}
			}

			return wtx.Commit()
		}); err != nil {
			return err
		}
		return nil
	}
}

// updateRelations, read values from read-only backup, write to new bucket.
func updateRelations(path bdb.Path, d ds.Direction) func(*zerolog.Logger, *bolt.DB, *bolt.DB) error {
	return func(log *zerolog.Logger, roDB *bolt.DB, rwDB *bolt.DB) error {
		log.Info().Str("version", Version).Msg("UpdateRelations")

		if err := roDB.View(func(rtx *bolt.Tx) error {
			wtx, err := rwDB.Begin(true)
			if err != nil {
				return err
			}
			defer wtx.Rollback()

			b, err := mig.SetBucket(rtx, path)
			if err != nil {
				return err
			}

			var newKey string

			c := b.Cursor()
			for key, value := c.First(); key != nil; key, value = c.Next() {
				if key == nil {
					break
				}

				r2, err := bdb.Unmarshal[dsc2.Relation](value)
				if err != nil {
					return err
				}

				r3 := convert.RelationToV3(r2)

				b3, err := bdb.Marshal[dsc3.Relation](r3)
				if err != nil {
					return err
				}

				if d == ds.ObjectToSubject {
					newKey = ds.Relation(r3).ObjKey()
				} else if d == ds.SubjectToObject {
					newKey = ds.Relation(r3).SubKey()
				}

				if err := mig.SetKey(wtx, path, []byte(newKey), b3); err != nil {
					return err
				}
			}
			return wtx.Commit()

		}); err != nil {
			return err
		}
		return nil
	}
}
