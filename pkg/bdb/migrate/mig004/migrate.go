package mig004

import (
	"bytes"
	"context"

	"github.com/aserto-dev/azm/model"
	v3 "github.com/aserto-dev/azm/v3"
	dsm3 "github.com/aserto-dev/go-directory/aserto/directory/model/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/rs/zerolog"

	bolt "go.etcd.io/bbolt"
)

// mig004
//
// load model from manifest and write it back to the db.
const (
	Version string = "0.0.4"
)

func Migrate(log *zerolog.Logger, roDB, rwDB *bolt.DB) error {
	logger := log.With().Str("version", Version).Logger()
	logger.Info().Msg("StartMigration")

	// skip when roDB is nil.
	if roDB == nil {
		logger.Debug().Msg("SKIP")
		return nil
	}

	ctx := context.Background()
	m, err := loadModel(ctx, roDB)
	if err != nil {
		return err
	}

	if err := rwDB.Update(func(tx *bolt.Tx) error {
		_, err := bdb.SetAny[model.Model](ctx, tx, bdb.ManifestPath, bdb.ModelKey, m)
		return err
	}); err != nil {
		return err
	}

	logger.Info().Msg("FinishedMigration")
	return nil
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
