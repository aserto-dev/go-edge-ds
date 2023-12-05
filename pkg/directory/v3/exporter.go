package v3

import (
	"fmt"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dse3 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v3"
	"github.com/aserto-dev/go-directory/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"google.golang.org/protobuf/types/known/structpb"

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

		// stats mode, short circuits when enabled
		if req.Options&uint32(dse3.Option_OPTION_STATS) != 0 {
			if err := exportStats(tx, stream, req.Options); err != nil {
				logger.Error().Err(err).Msg("export_stats")
				return err
			}
			return nil
		}

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
	iter, err := bdb.NewScanIterator[dsc3.Object](stream.Context(), tx, bdb.ObjectsPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse3.ExportResponse{Msg: &dse3.ExportResponse_Object{Object: iter.Value()}}); err != nil {
			return err
		}
	}

	return nil
}

func exportRelations(tx *bolt.Tx, stream dse3.Exporter_ExportServer) error {
	iter, err := bdb.NewScanIterator[dsc3.Relation](stream.Context(), tx, bdb.RelationsObjPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		if err := stream.Send(&dse3.ExportResponse{Msg: &dse3.ExportResponse_Relation{Relation: iter.Value()}}); err != nil {
			return err
		}
	}

	return nil
}

func exportStats(tx *bolt.Tx, stream dse3.Exporter_ExportServer, opts uint32) error {
	stats := pb.NewStruct()

	// object stats.
	if opts&uint32(dse3.Option_OPTION_DATA_OBJECTS) != 0 {
		iter, err := bdb.NewScanIterator[dsc3.Object](stream.Context(), tx, bdb.ObjectsPath)
		if err != nil {
			return err
		}

		for iter.Next() {
			objectType := iter.Value().Type

			f, ok := stats.Fields[objectType]
			if !ok {
				sv, err := structpb.NewValue(map[string]interface{}{})
				if err != nil {
					return err
				}
				sv.Fields["_obj_count"], err = structpb.NewValue(uint32(0))
				structpb.NewStructValue()
				stats.Fields[objectType], err = structpb.NewValue(uint32(0))
			}

			// if _, ok := stats[objectType]; !ok {
			// 	stats[objectType] = 0
			// }
			// counter := stats[objectType].(int)
			// counter++
			// stats[objectType] = counter
		}
	}

	// relation stats.
	if opts&uint32(dse3.Option_OPTION_DATA_RELATIONS) != 0 {
		iter, err := bdb.NewScanIterator[dsc3.Relation](stream.Context(), tx, bdb.RelationsObjPath)
		if err != nil {
			return err
		}

		for iter.Next() {
			objectType := iter.Value().ObjectType
			// if _, ok := stats[objectType]; !ok {
			// 	stats[objectType] = 0
			// }

			// counter := stats[objectType].(int)
			// counter++
			// stats[objectType] = counter

			// relation := iter.Value().Relation
			// if _, ok := stats[objectType][relation]; !ok {
			// 	stats[objectType] = 0
			// }

		}
	}

	s, err := structpb.NewStruct(stats)
	if err != nil {
		return err
	}

	if err := stream.Send(&dse3.ExportResponse{Msg: &dse3.ExportResponse_Stats{Stats: s}}); err != nil {
		return err
	}

	return nil
}

func zStr(r *dsc3.Relation) string {
	return fmt.Sprintf("%s#%s@%s%s", r.ObjectType, r.Relation, r.SubjectType, ds.Iff(r.SubjectRelation != "", fmt.Sprintf("#%s", r.SubjectRelation), ""))
}
