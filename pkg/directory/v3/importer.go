package v3

import (
	dsi3 "github.com/aserto-dev/go-directory/aserto/directory/importer/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/rs/zerolog"
)

type Importer struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
}

func NewImporter(logger *zerolog.Logger, store *bdb.BoltDB) *Importer {
	return &Importer{
		logger: logger,
		store:  store,
	}
}

func (s *Importer) Import(dsi3.Importer_ImportServer) error {
	return nil
}
