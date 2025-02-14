package mig008

import (
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig"
	"github.com/rs/zerolog"

	bolt "go.etcd.io/bbolt"
)

// mig008
//
// reload model from manifest and write new model back to db.
const (
	Version string = "0.0.8"
)

var fnMap = []func(*zerolog.Logger, *bolt.DB, *bolt.DB) error{
	mig.CreateBucket(bdb.SystemPath),

	mig.CreateBucket(bdb.ManifestPath),
	mig.MigrateModel,

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
