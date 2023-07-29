package v3

import (
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dse3 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
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
	logger := s.logger.With().Str("method", "Export").Interface("req", req).Logger()

	err := s.store.DB().View(func(tx *bolt.Tx) error {

		if req.Options&uint32(dse3.Option_OPTION_DATA_OBJECTS) != 0 {
			if err := exportObjects(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_objects")
				return err
			}
		}

		if req.Options&uint32(dse3.Option_OPTION_DATA_RELATIONS) != 0 {
			if err := exportRelations(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_relations")
				return err
			}
		}

		return nil
	})

	return err
}

func exportObjects(tx *bolt.Tx, stream dse3.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc2.Object](stream.Context(), tx, bdb.ObjectsPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse3.ExportResponse{
			Msg: &dse3.ExportResponse_Object{
				Object: &dsc3.Object{
					Type:        iter.Value().GetType(),
					Id:          iter.Value().GetKey(),
					DisplayName: iter.Value().GetDisplayName(),
					Properties:  iter.Value().GetProperties(),
					CreatedAt:   iter.Value().GetCreatedAt(),
					UpdatedAt:   iter.Value().GetUpdatedAt(),
					Etag:        iter.Value().GetHash(),
				},
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func exportRelations(tx *bolt.Tx, stream dse3.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc2.Relation](stream.Context(), tx, bdb.RelationsObjPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse3.ExportResponse{
			Msg: &dse3.ExportResponse_Relation{
				Relation: &dsc3.Relation{
					ObjectType:  iter.Value().GetObject().GetType(),
					ObjectId:    iter.Value().GetObject().GetKey(),
					Relation:    iter.Value().GetRelation(),
					SubjectType: iter.Value().GetSubject().GetType(),
					SubjectId:   iter.Value().GetSubject().GetKey(),
					CreatedAt:   iter.Value().GetCreatedAt(),
					UpdatedAt:   iter.Value().GetUpdatedAt(),
					Etag:        iter.Value().GetHash(),
				},
			},
		}); err != nil {
			return err
		}
	}

	return nil
}
