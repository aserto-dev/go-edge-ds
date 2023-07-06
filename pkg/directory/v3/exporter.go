package v3

import (
	dse3 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/rs/zerolog"
)

type Exporter struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
}

func NewExporter(logger *zerolog.Logger, store *bdb.BoltDB) *Exporter {
	return &Exporter{
		logger: logger,
		store:  store,
	}
}

func (s *Exporter) Export(req *dse3.ExportRequest, stream dse3.Exporter_ExportServer) error {
	return nil
}
