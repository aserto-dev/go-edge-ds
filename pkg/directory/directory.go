package directory

import (
	"time"

	azm "github.com/aserto-dev/azm"
	dse2 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	dsi2 "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrate"
	v2 "github.com/aserto-dev/go-edge-ds/pkg/directory/v2"

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
	config    *Config
	logger    *zerolog.Logger
	store     *bdb.BoltDB
	model     *azm.Model
	exporter2 dse2.ExporterServer
	importer2 dsi2.ImporterServer
	reader2   dsr2.ReaderServer
	writer2   dsw2.WriterServer
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

	model, err := store.LoadModel()
	if err != nil {
		return nil, err
	}

	reader2 := v2.NewReader(logger, store, model)
	writer2 := v2.NewWriter(logger, store, model)

	dir := &Directory{
		config:    config,
		logger:    &newLogger,
		store:     store,
		model:     model,
		exporter2: v2.NewExporter(logger, store, model),
		importer2: v2.NewImporter(logger, store, model),
		reader2:   reader2,
		writer2:   writer2,
	}

	if err := dir.Migrate(schemaVersion); err != nil {
		return nil, err
	}

	model, err := store.LoadModel()
	if err != nil {
		return nil, err
	}

	dir.model.Dump("./model.json")

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
