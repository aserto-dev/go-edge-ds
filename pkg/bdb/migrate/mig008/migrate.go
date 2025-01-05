package mig008

import (
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate/mig"

	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

// mig008
//
// #1 add RelationPath to store relations, copy values from RelationsObjPath.
// #2 remove values from RelationsObjPath and RelationsSubPath, set to nil.
const (
	Version string = "0.0.8"
)

var fnMap = []func(*zerolog.Logger, *bolt.DB, *bolt.DB) error{
	mig.CreateBucket(bdb.RelationsPath),
	copyRelations(),

	removeValues(bdb.RelationsObjPath),
	removeValues(bdb.RelationsSubPath),
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

// copyRelations, copy relations from RelationsObjPath to RelationsPath.
func copyRelations() func(*zerolog.Logger, *bolt.DB, *bolt.DB) error {
	return func(log *zerolog.Logger, roDB *bolt.DB, rwDB *bolt.DB) error {
		log.Info().Str("version", Version).Msg("copyRelations")
		if roDB == nil {
			log.Info().Bool("roDB", roDB == nil).Msg("copyRelations")
			return nil
		}

		if err := roDB.View(func(rtx *bolt.Tx) error {
			wtx, err := rwDB.Begin(true)
			if err != nil {
				return err
			}
			defer func() { _ = wtx.Rollback() }()

			b, err := mig.SetBucket(rtx, bdb.RelationsObjPath)
			if err != nil {
				return err
			}

			c := b.Cursor()
			for key, value := c.First(); key != nil; key, value = c.Next() {
				if err := mig.SetKey(wtx, bdb.RelationsPath, key, value); err != nil {
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

// removeValues, zero out values in bucket.
func removeValues(path bdb.Path) func(*zerolog.Logger, *bolt.DB, *bolt.DB) error {
	return func(log *zerolog.Logger, roDB *bolt.DB, rwDB *bolt.DB) error {
		log.Info().Str("version", Version).Msg("removeValues")
		if roDB == nil {
			log.Info().Bool("roDB", roDB == nil).Msg("removeValues")
			return nil
		}

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
			for key, _ := c.First(); key != nil; key, _ = c.Next() {
				if err := mig.SetKey(wtx, path, key, []byte{}); err != nil {
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
