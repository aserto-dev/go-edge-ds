package directory

import (
	"context"
	"time"

	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/directory/metadata"
	"github.com/aserto-dev/go-edge-ds/pkg/session"
	"github.com/aserto-dev/go-edge-ds/pkg/types"
	"github.com/google/uuid"

	"github.com/rs/zerolog"
)

type Config struct {
	DBPath         string        `json:"db_path"`
	RequestTimeout time.Duration `json:"request_timeout"`
	Seed           bool          `json:"seed_metadata"`
}

type Directory struct {
	config *Config
	logger *zerolog.Logger
	store  *boltdb.BoltDB
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

	ds := &Directory{
		config: config,
		logger: &newLogger,
		store:  store,
	}

	if err := ds.Init(); err != nil {
		return nil, err
	}

	if err := ds.Seed(); err != nil {
		return nil, err
	}

	return ds, nil
}

func (s *Directory) Close() {
	if s.store != nil {
		s.store.Close()
		s.store = nil
	}
}

func (s *Directory) Init() error {
	paths := []func() []string{
		types.ObjectTypesPath,
		types.ObjectTypesNamePath,
		types.PermissionsPath,
		types.PermissionsNamePath,
		types.RelationTypesPath,
		types.RelationTypesNamePath,
		types.ObjectsPath,
		types.ObjectsKeyPath,
		types.RelationsSubPath,
		types.RelationsObjPath,
	}

	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	for _, path := range paths {
		if err := s.store.CreateBucket(path(), []boltdb.Opts{txOpt}); err != nil {
			return err
		}
	}

	return nil
}

func (s *Directory) Seed() error {
	if !s.config.Seed {
		return nil
	}

	ctx := session.ContextWithSessionID(context.Background(), uuid.NewString())

	for _, objType := range metadata.ObjectTypes {
		_, err := s.SetObjectType(ctx, &dsw.SetObjectTypeRequest{ObjectType: objType})
		if err != nil {
			return err
		}
	}

	for _, relType := range metadata.RelationTypes {
		_, err := s.SetRelationType(ctx, &dsw.SetRelationTypeRequest{RelationType: relType})
		if err != nil {
			return err
		}
	}

	return nil
}
