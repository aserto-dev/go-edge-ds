package mig005

import (
	"bytes"
	"context"

	"github.com/aserto-dev/azm/model"
	v3 "github.com/aserto-dev/azm/v3"
	dsm3 "github.com/aserto-dev/go-directory/aserto/directory/model/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig"
	"github.com/rs/zerolog"

	bolt "go.etcd.io/bbolt"
)

// mig004
//
// reload model from manifest and write new model back to db.
const (
	Version string = "0.0.5"
)

var fnMap = []func(*zerolog.Logger, *bolt.DB, *bolt.DB) error{
	mig.CreateBucket(bdb.SystemPath),

	mig.CreateBucket(bdb.ManifestPath),
	migrateModel(),

	mig.CreateBucket(bdb.ObjectsPath),
	mig.CreateBucket(bdb.RelationsObjPath),
	mig.CreateBucket(bdb.RelationsSubPath),
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

func migrateModel() func(*zerolog.Logger, *bolt.DB, *bolt.DB) error {
	return func(log *zerolog.Logger, roDB *bolt.DB, rwDB *bolt.DB) error {
		// skip when roDB is nil.
		if roDB == nil {
			log.Debug().Msg("SKIP MigrateModel")
			return nil
		}

		ctx := context.Background()
		m, err := loadModel(ctx, roDB)
		if err != nil {
			return err
		}

		if err := rwDB.Update(func(tx *bolt.Tx) error {
			_, err := bdb.SetAny(ctx, tx, bdb.ManifestPath, bdb.ModelKey, m)
			return err
		}); err != nil {
			return err
		}

		return nil
	}
}

func loadModel(ctx context.Context, roDB *bolt.DB) (*model.Model, error) {
	var m *model.Model
	if err := roDB.View(func(rtx *bolt.Tx) error {
		manifestBody, err := bdb.Get[dsm3.Body](ctx, rtx, bdb.ManifestPath, bdb.BodyKey)
		if err != nil {
			return err
		}

		m, err = v3.Load(bytes.NewReader(manifestBody.Data))
		return err
	}); err != nil {
		return m, err
	}

	return m, nil
}
