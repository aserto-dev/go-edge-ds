package directory

import (
	"context"
	"time"

	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/directory/migrate"
	"github.com/aserto-dev/go-edge-ds/pkg/model"
	bolt "go.etcd.io/bbolt"

	"github.com/rs/zerolog"
)

// required minimum schema version, when the current version is lower, migration will be invoked to update to the minimum schema version required.
const schemaVersion string = "0.0.2"

type Config struct {
	DBPath         string        `json:"db_path"`
	RequestTimeout time.Duration `json:"request_timeout"`
	Seed           bool          `json:"seed_metadata"`
}

type Directory struct {
	config   *Config
	logger   *zerolog.Logger
	store    *boltdb.BoltDB
	resolver *model.Model
}

func New(config *Config, logger *zerolog.Logger) (*Directory, error) {
	newLogger := logger.With().Str("component", "directory").Logger()

	store, err := boltdb.New(&boltdb.Config{
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

	resolver, err := model.NewResolver()
	if err != nil {
		return nil, err
	}

	if err := dir.store.DB().View(func(tx *bolt.Tx) error {
		return resolver.Update(context.Background(), tx)
	}); err != nil {
		return nil, err
	}

	dir.resolver = resolver

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
