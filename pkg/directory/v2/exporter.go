package v2

import (
	"github.com/aserto-dev/azm"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dse "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Exporter struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	model  *azm.Model
}

func NewExporter(logger *zerolog.Logger, store *bdb.BoltDB, model *azm.Model) *Exporter {
	return &Exporter{
		logger: logger,
		store:  store,
		model:  model,
	}
}

func (s *Exporter) Export(req *dse.ExportRequest, stream dse.Exporter_ExportServer) error {
	logger := s.logger.With().Str("method", "Export").Interface("req", req).Logger()

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		if req.Options&uint32(dse.Option_OPTION_METADATA_OBJECT_TYPES) != 0 {
			if err := exportObjectTypes(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_object_types")
				return err
			}
		}

		if req.Options&uint32(dse.Option_OPTION_METADATA_RELATION_TYPES) != 0 {
			if err := exportRelationTypes(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_relation_types")
				return err
			}
		}

		if req.Options&uint32(dse.Option_OPTION_METADATA_PERMISSIONS) != 0 {
			if err := exportPermissions(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_permissions")
				return err
			}
		}

		if req.Options&uint32(dse.Option_OPTION_DATA_OBJECTS) != 0 {
			if err := exportObjects(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_objects")
				return err
			}
		}

		if req.Options&uint32(dse.Option_OPTION_DATA_RELATIONS) != 0 {
			if err := exportRelations(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_relations")
				return err
			}
		}

		return nil
	})

	return err
}

func exportObjectTypes(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc.ObjectType](stream.Context(), tx, bdb.ObjectTypesPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse.ExportResponse{
			Msg: &dse.ExportResponse_ObjectType{
				ObjectType: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func exportRelationTypes(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc.RelationType](stream.Context(), tx, bdb.RelationTypesPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse.ExportResponse{
			Msg: &dse.ExportResponse_RelationType{
				RelationType: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func exportPermissions(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc.Permission](stream.Context(), tx, bdb.PermissionsPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse.ExportResponse{
			Msg: &dse.ExportResponse_Permission{
				Permission: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func exportObjects(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc.Object](stream.Context(), tx, bdb.ObjectsPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse.ExportResponse{
			Msg: &dse.ExportResponse_Object{
				Object: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func exportRelations(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc.Relation](stream.Context(), tx, bdb.RelationsObjPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse.ExportResponse{
			Msg: &dse.ExportResponse_Relation{
				Relation: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}
