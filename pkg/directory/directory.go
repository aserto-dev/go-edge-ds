package directory

import (
	"context"
	"time"

	azm "github.com/aserto-dev/azm"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	bolt "go.etcd.io/bbolt"

	"github.com/rs/zerolog"
)

// required minimum schema version, when the current version is lower, migration will be invoked to update to the minimum schema version required.
const (
	schemaVersion   string = "0.0.2"
	manifestVersion int    = 2
	manifestName    string = "edge"
)

type Config struct {
	DBPath         string        `json:"db_path"`
	RequestTimeout time.Duration `json:"request_timeout"`
	Seed           bool          `json:"seed_metadata"`
}

type Directory struct {
	config *Config
	logger *zerolog.Logger
	store  *bdb.BoltDB
	model  *azm.Model
}

func New(config *Config, logger *zerolog.Logger) (*Directory, error) {
	newLogger := logger.With().Str("component", "directory").Logger()

	store, err := bdb.New(&bdb.Config{
		DBPath:         config.DBPath,
		RequestTimeout: config.RequestTimeout},
		&newLogger,
	)
	if err != nil {
		return nil, err
	}

	if err := store.Open(); err != nil {
		return nil, err
	}

	dir := &Directory{
		config: config,
		logger: &newLogger,
		store:  store,
	}

	if err := dir.Migrate(schemaVersion); err != nil {
		return nil, err
	}

	model := azm.New(manifestName, manifestVersion)
	if err != nil {
		return nil, err
	}

	if err := dir.store.DB().View(func(tx *bolt.Tx) error {
		dir.model, err = ds.Model(model).Update(context.Background(), tx)
		return err
	}); err != nil {
		return nil, err
	}

	return dir, nil
}

func (s *Directory) Close() {
	if s.store != nil {
		s.store.Close()
		s.store = nil
	}
}

func (s *Directory) Migrate(version string) error {
	return migrate.Store(s.logger, s.store, version)
}

func (s *Directory) Model() *azm.Model {
	return s.model
}
