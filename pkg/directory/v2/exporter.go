package v2

import (
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dse2 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	dse3 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	v3 "github.com/aserto-dev/go-edge-ds/pkg/directory/v3"

	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Exporter struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	e3     dse3.ExporterServer
}

func NewExporter(logger *zerolog.Logger, store *bdb.BoltDB, e3 *v3.Exporter) *Exporter {
	return &Exporter{
		logger: logger,
		store:  store,
		e3:     e3,
	}
}

func (s *Exporter) Export(req *dse2.ExportRequest, stream dse2.Exporter_ExportServer) error {
	logger := s.logger.With().Str("method", "Export").Interface("req", req).Logger()

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		if req.Options&uint32(dse2.Option_OPTION_METADATA_OBJECT_TYPES) != 0 {
			if err := exportObjectTypes(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_object_types")
				return err
			}
		}

		if req.Options&uint32(dse2.Option_OPTION_METADATA_RELATION_TYPES) != 0 {
			if err := exportRelationTypes(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_relation_types")
				return err
			}
		}

		if req.Options&uint32(dse2.Option_OPTION_METADATA_PERMISSIONS) != 0 {
			if err := exportPermissions(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_permissions")
				return err
			}
		}

		if req.Options&uint32(dse2.Option_OPTION_DATA_OBJECTS) != 0 {
			if err := exportObjects(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_objects")
				return err
			}
		}

		if req.Options&uint32(dse2.Option_OPTION_DATA_RELATIONS) != 0 {
			if err := exportRelations(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_relations")
				return err
			}
		}

		return nil
	})

	return err
}

func exportObjectTypes(tx *bolt.Tx, stream dse2.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc2.ObjectType](stream.Context(), tx, bdb.ObjectTypesPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse2.ExportResponse{
			Msg: &dse2.ExportResponse_ObjectType{
				ObjectType: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func exportRelationTypes(tx *bolt.Tx, stream dse2.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc2.RelationType](stream.Context(), tx, bdb.RelationTypesPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse2.ExportResponse{
			Msg: &dse2.ExportResponse_RelationType{
				RelationType: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func exportPermissions(tx *bolt.Tx, stream dse2.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc2.Permission](stream.Context(), tx, bdb.PermissionsPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse2.ExportResponse{
			Msg: &dse2.ExportResponse_Permission{
				Permission: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func exportObjects(tx *bolt.Tx, stream dse2.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc2.Object](stream.Context(), tx, bdb.ObjectsPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse2.ExportResponse{
			Msg: &dse2.ExportResponse_Object{
				Object: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func exportRelations(tx *bolt.Tx, stream dse2.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc2.Relation](stream.Context(), tx, bdb.RelationsObjPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse2.ExportResponse{
			Msg: &dse2.ExportResponse_Relation{
				Relation: iter.Value(),
			},
		}); err != nil {
			return err
		}
	}

	return nil
}
