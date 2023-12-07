package v3

import (
	"encoding/json"
	"sync/atomic"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dse3 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v3"
	"github.com/aserto-dev/go-directory/pkg/pb"
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
	stats := &Stats{ObjectTypes: ObjectTypes{}}

	// object stats.
	if opts&uint32(dse3.Option_OPTION_DATA_OBJECTS) != 0 {
		iter, err := bdb.NewScanIterator[dsc3.Object](stream.Context(), tx, bdb.ObjectsPath)
		if err != nil {
			return err
		}

		for iter.Next() {
			obj := iter.Value()
			stats.CountObject(obj)
		}
	}

	// relation stats.
	if opts&uint32(dse3.Option_OPTION_DATA_RELATIONS) != 0 {
		iter, err := bdb.NewScanIterator[dsc3.Relation](stream.Context(), tx, bdb.RelationsObjPath)
		if err != nil {
			return err
		}

		for iter.Next() {
			rel := iter.Value()
			stats.CountRelation(rel)
		}
	}

	buf, err := json.Marshal(stats)
	if err != nil {
		return err
	}

	resp := pb.NewStruct()
	if err := resp.UnmarshalJSON(buf); err != nil {
		return err
	}

	if err := stream.Send(&dse3.ExportResponse{Msg: &dse3.ExportResponse_Stats{Stats: resp}}); err != nil {
		return err
	}

	return nil
}

type ObjType string
type Relation string
type SubType string
type SubRel string

type Stats struct {
	ObjectTypes ObjectTypes `json:"object_types,omitempty"`
}

type ObjectTypes map[ObjType]struct {
	ObjCount  int32     `json:"_obj_count,omitempty"`
	Count     int32     `json:"_count,omitempty"`
	Relations Relations `json:"relations,omitempty"`
}

type Relations map[Relation]struct {
	Count        int32        `json:"_count,omitempty"`
	SubjectTypes SubjectTypes `json:"subject_types,omitempty"`
}

type SubjectTypes map[SubType]struct {
	Count            int32            `json:"_count,omitempty"`
	SubjectRelations SubjectRelations `json:"subject_relations,omitempty"`
}

type SubjectRelations map[SubRel]struct {
	Count int32 `json:"_count,omitempty"`
}

func (s *Stats) CountObject(obj *dsc3.Object) {
	ot, ok := s.ObjectTypes[ObjType(obj.Type)]
	if !ok {
		atomic.StoreInt32(&ot.ObjCount, 0)
		if ot.Relations == nil {
			ot.Relations = Relations{}
		}
	}

	atomic.AddInt32(&ot.ObjCount, 1)

	s.ObjectTypes[ObjType(obj.Type)] = ot
}

func (s *Stats) CountRelation(rel *dsc3.Relation) {
	objType := ObjType(rel.ObjectType)
	relation := Relation(rel.Relation)
	subType := SubType(rel.SubjectType)
	subRel := SubRel(rel.SubjectRelation)

	// object_types
	ot, ok := s.ObjectTypes[objType]
	if !ok {
		atomic.StoreInt32(&ot.Count, 0)
	}

	if ot.Relations == nil {
		ot.Relations = Relations{}
	}

	atomic.AddInt32(&ot.Count, 1)
	s.ObjectTypes[objType] = ot

	// relations
	re, ok := ot.Relations[relation]
	if !ok {
		atomic.StoreInt32(&re.Count, 0)
		re.SubjectTypes = SubjectTypes{}
	}

	atomic.AddInt32(&re.Count, 1)
	ot.Relations[relation] = re

	// subject_types
	st, ok := re.SubjectTypes[subType]
	if !ok {
		atomic.StoreInt32(&st.Count, 0)
		st.SubjectRelations = SubjectRelations{}
	}

	atomic.AddInt32(&st.Count, 1)
	re.SubjectTypes[subType] = st

	// subject_relations
	if subRel != "" {
		sr, ok := st.SubjectRelations[subRel]
		if !ok {
			atomic.StoreInt32(&sr.Count, 0)
		}

		atomic.AddInt32(&sr.Count, 1)
		st.SubjectRelations[subRel] = sr
	}
}
