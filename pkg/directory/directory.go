package directory

import (
	"context"
	"errors"
	"sync"
	"time"

	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
	dsa "github.com/authzen/access.go/api/access/v1"

	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb/migrations/migrate"
	"github.com/aserto-dev/go-edge-ds/pkg/datasync"
	v3 "github.com/aserto-dev/go-edge-ds/pkg/directory/v3"

	"github.com/Masterminds/semver/v3"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// required minimum schema version, when the current version is lower,
// migration will be invoked to update to the minimum schema version required.
const (
	schemaVersion   string = "0.0.9"
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
	config *Config
	logger *zerolog.Logger
	store  *bdb.BoltDB
	reader dsr.ReaderServer
	writer dsw.WriterServer
	access dsa.AccessServer
}

var (
	directory *Directory
	once      sync.Once
)

func Get() (*Directory, error) {
	if directory != nil {
		return directory, nil
	}

	return nil, status.Error(codes.Internal, "directory not initialized")
}

func New(ctx context.Context, config *Config, logger *zerolog.Logger) (*Directory, error) {
	var err error

	once.Do(func() {
		directory, err = newDirectory(ctx, config, logger)
	})

	return directory, err
}

func newDirectory(_ context.Context, config *Config, logger *zerolog.Logger) (*Directory, error) {
	newLogger := logger.With().Str("component", "directory").Logger()

	cfg := bdb.Config{
		DBPath:         config.DBPath,
		RequestTimeout: config.RequestTimeout,
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
	},
		&newLogger,
	)
	if err != nil {
		return nil, err
	}

	if err := store.Open(); err != nil {
		return nil, err
	}

	reader := v3.NewReader(logger, store)
	writer := v3.NewWriter(logger, store)
	access := v3.NewAccess(logger, reader)

	dir := &Directory{
		config: config,
		logger: &newLogger,
		store:  store,
		reader: reader,
		writer: writer,
		access: access,
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

func (s *Directory) Reader3() dsr.ReaderServer {
	return s.reader
}

func (s *Directory) Writer3() dsw.WriterServer {
	return s.writer
}

func (s *Directory) Access1() dsa.AccessServer {
	return s.access
}

func (s *Directory) Logger() *zerolog.Logger {
	return s.logger
}

func (s *Directory) Config() Config {
	return *s.config
}

func (s *Directory) DataSyncClient() datasync.SyncClient {
	return datasync.New(s.logger, s.store)
}
