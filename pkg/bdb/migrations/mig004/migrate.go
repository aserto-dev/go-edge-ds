package mig004

import (
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrations/common"

	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

// mig004
//
// reload model from manifest and write new model back to db.
const (
	Version string = "0.0.4"
)

var fnMap = []func(*zerolog.Logger, *bolt.DB, *bolt.DB) error{
	common.CreateBucket(bdb.SystemPath),

	common.CreateBucket(bdb.ManifestPathV1),
	common.MigrateModelV1,

	common.CreateBucket(bdb.ObjectsPath),
	common.CreateBucket(bdb.RelationsObjPath),
	common.CreateBucket(bdb.RelationsSubPath),
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
