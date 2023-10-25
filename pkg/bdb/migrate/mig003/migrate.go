package mig003

import (
	"bytes"
	"context"
	"os"
	"path/filepath"

	"github.com/aserto-dev/azm/migrate"
	v3 "github.com/aserto-dev/azm/v3"
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsm3 "github.com/aserto-dev/go-directory/aserto/directory/model/v3"
	"github.com/aserto-dev/go-directory/pkg/convert"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

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
// 2) persists the manifest file byte-stream in the store
// 3) persists the serialized model in the store.
func migrateModel() func(*zerolog.Logger, *bolt.DB, *bolt.DB) error {
	return func(log *zerolog.Logger, roDB *bolt.DB, rwDB *bolt.DB) error {

		log.Info().Str("version", Version).Msg("MigrateModel")
		ctx := context.Background()

		metadata, err := getMetadata(ctx, roDB)
		if err != nil {
			return err
		}

		relationMap, err := relationMap(roDB)
		if err != nil {
			return err
		}

		m := &migrate.Migrator{
			Metadata:      metadata,
			RelationMap:   relationMap,
			PermissionMap: migrate.NewObjPermRelContainer(),
		}

		if err := m.Process(); err != nil {
			return err
		}

		manifestBuf := new(bytes.Buffer)
		if err := m.Write(manifestBuf); err != nil {
			return err
		}

		model, err := v3.Load(bytes.NewReader(manifestBuf.Bytes()))
		if err != nil {
			return err
		}

		md := &dsm3.Metadata{
			UpdatedAt: timestamppb.Now(),
		}
		if err := rwDB.Update(func(tx *bolt.Tx) error {
			if err := ds.Manifest(md).Set(ctx, tx, manifestBuf); err != nil {
				return errors.Errorf("failed to set manifest")
			}

			if err := ds.Manifest(md).SetModel(ctx, tx, model); err != nil {
				return errors.Errorf("failed to set model")
			}

			return nil
		}); err != nil {
			return err
		}

		fileName := manifestFilename(rwDB.Path(), "manifest.yaml")
		w, err := os.Create(fileName)
		if err != nil {
			return err
		}
		defer w.Close()

		if _, err := manifestBuf.WriteTo(w); err != nil {
			return err
		}

		log.Info().Str("manifest", fileName).Msg("write migrated manifest")
		return nil
	}
}

func getMetadata(ctx context.Context, roDB *bolt.DB) (*migrate.Metadata, error) {
	metadata := &migrate.Metadata{}
	if err := roDB.View(func(rtx *bolt.Tx) error {
		objectTypes, err := bdb.List[dsc2.ObjectType](ctx, rtx, bdb.ObjectTypesPath)
		if err != nil {
			return err
		}

		relationTypes, err := bdb.List[dsc2.RelationType](ctx, rtx, bdb.RelationTypesPath)
		if err != nil {
			return err
		}

		permissions, err := bdb.List[dsc2.Permission](ctx, rtx, bdb.PermissionsPath)
		if err != nil {
			return err
		}

		metadata = &migrate.Metadata{
			ObjectTypes:   objectTypes,
			RelationTypes: relationTypes,
			Permissions:   permissions,
		}

		return nil
	}); err != nil {
		return nil, err
	}
	return metadata, nil
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
			defer func() { _ = wtx.Rollback() }()

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
			defer func() { _ = wtx.Rollback() }()

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

func relationMap(roDB *bolt.DB) (*migrate.ObjRelSubContainer, error) {
	orsc := migrate.NewObjRelSubContainer()

	if err := roDB.View(func(rtx *bolt.Tx) error {
		b, err := mig.SetBucket(rtx, bdb.RelationsObjPath)
		if err != nil {
			return err
		}

		c := b.Cursor()
		for key, value := c.First(); key != nil; key, value = c.Next() {
			if key == nil {
				break
			}

			r2, err := bdb.Unmarshal[dsc2.Relation](value)
			if err != nil {
				return err
			}

			orsc.Add(&migrate.ObjRelSub{
				Object:   r2.GetObject().GetType(),
				Relation: r2.GetRelation(),
				Subject:  r2.GetSubject().GetType(),
			})
		}

		return nil

	}); err != nil {
		return nil, err
	}

	return orsc, nil
}

func manifestFilename(dbPath, name string) string {
	dir, _ := filepath.Split(dbPath)
	return filepath.Join(dir, name)
}
