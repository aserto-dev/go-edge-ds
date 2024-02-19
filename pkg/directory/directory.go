package directory

import (
	"context"
	"errors"
	"time"

	dse2 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	dsi2 "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"

	dse3 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v3"
	dsi3 "github.com/aserto-dev/go-directory/aserto/directory/importer/v3"
	dsm3 "github.com/aserto-dev/go-directory/aserto/directory/model/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"

	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate"
	v2 "github.com/aserto-dev/go-edge-ds/pkg/directory/v2"
	v3 "github.com/aserto-dev/go-edge-ds/pkg/directory/v3"

	"github.com/Masterminds/semver"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

// required minimum schema version, when the current version is lower, migration will be invoked to update to the minimum schema version required.
const (
	schemaVersion   string = "0.0.4"
	manifestVersion int    = 2
	manifestName    string = "edge"
)

type Config struct {
	DBPath         string        `json:"db_path"`
	RequestTimeout time.Duration `json:"request_timeout"`
	Seed           bool          `json:"seed_metadata"`
	EnableV2       bool          `json:"enable_v2"`
}

type Directory struct {
	config    *Config
	logger    *zerolog.Logger
	store     *bdb.BoltDB
	exporter2 dse2.ExporterServer
	importer2 dsi2.ImporterServer
	reader2   dsr2.ReaderServer
	writer2   dsw2.WriterServer
	exporter3 dse3.ExporterServer
	importer3 dsi3.ImporterServer
	model3    dsm3.ModelServer
	reader3   dsr3.ReaderServer
	writer3   dsw3.WriterServer
}

func New(ctx context.Context, config *Config, logger *zerolog.Logger) (*Directory, error) {
	newLogger := logger.With().Str("component", "directory").Logger()

	cfg := bdb.Config{
		DBPath:         config.DBPath,
		RequestTimeout: config.RequestTimeout,
		MaxBatchSize:   bolt.DefaultMaxBatchSize,
		MaxBatchDelay:  bolt.DefaultMaxBatchDelay,
	}

	if ok, err := migrate.CheckSchemaVersion(&cfg, logger, semver.MustParse(schemaVersion)); !ok {
		switch {
		case errors.Is(err, migrate.ErrDirectorySchemaUpdateRequired):
			if err := migrate.Migrate(&cfg, logger, semver.MustParse(schemaVersion)); err != nil {
				return nil, err
			}
		case errors.Is(err, migrate.ErrDirectorySchemaVersionHigher):
			return nil, err
		default:
			return nil, err
		}

		if ok, err := migrate.CheckSchemaVersion(&cfg, logger, semver.MustParse(schemaVersion)); !ok {
			return nil, err
		}
	}

	store, err := bdb.New(&bdb.Config{
		DBPath:         config.DBPath,
		RequestTimeout: config.RequestTimeout,
		MaxBatchSize:   bolt.DefaultMaxBatchSize,
		MaxBatchDelay:  bolt.DefaultMaxBatchDelay},
		&newLogger,
	)
	if err != nil {
		return nil, err
	}

	if err := store.Open(); err != nil {
		return nil, err
	}

	reader3 := v3.NewReader(logger, store)
	writer3 := v3.NewWriter(logger, store)
	exporter3 := v3.NewExporter(logger, store)
	importer3 := v3.NewImporter(logger, store)

	dir := &Directory{
		config:    config,
		logger:    &newLogger,
		store:     store,
		model3:    v3.NewModel(logger, store),
		reader3:   reader3,
		writer3:   writer3,
		exporter3: exporter3,
		importer3: importer3,
	}

	if config.EnableV2 {
		dir.exporter2 = v2.NewExporter(logger, store, exporter3)
		dir.importer2 = v2.NewImporter(logger, store, importer3)
		dir.reader2 = v2.NewReader(logger, store, reader3)
		dir.writer2 = v2.NewWriter(logger, store, writer3)
	}

	if err := store.LoadModel(); err != nil {
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

func (s *Directory) Exporter2() dse2.ExporterServer {
	return s.exporter2
}

func (s *Directory) Importer2() dsi2.ImporterServer {
	return s.importer2
}

func (s *Directory) Reader2() dsr2.ReaderServer {
	return s.reader2
}

func (s *Directory) Writer2() dsw2.WriterServer {
	return s.writer2
}

func (s *Directory) Exporter3() dse3.ExporterServer {
	return s.exporter3
}

func (s *Directory) Importer3() dsi3.ImporterServer {
	return s.importer3
}

func (s *Directory) Model3() dsm3.ModelServer {
	return s.model3
}

func (s *Directory) Reader3() dsr3.ReaderServer {
	return s.reader3
}

func (s *Directory) Writer3() dsw3.WriterServer {
	return s.writer3
}

func (s *Directory) Logger() *zerolog.Logger {
	return s.logger
}

// Config, returns read-only copy of directory configuration data.
func (s *Directory) Config() Config {
	return *s.config
}
