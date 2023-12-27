package ds

import (
	"context"
	"sync/atomic"

	"github.com/aserto-dev/azm/model"
	"github.com/aserto-dev/azm/stats"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	bolt "go.etcd.io/bbolt"
)

// CalculateStats returns a Stats object with the counts of all objects and relations.
func CalculateStats(ctx context.Context, tx *bolt.Tx) (*stats.Stats, error) {
	s := NewStats()

	if err := s.CountObjects(ctx, tx); err != nil {
		return nil, err
	}

	if err := s.CountRelations(ctx, tx); err != nil {
		return nil, err
	}

	return s.Stats, nil
}

// Wraps the azm Stats object and adds mutator methods.
type Stats struct {
	*stats.Stats
}

func NewStats() *Stats {
	return &Stats{Stats: &stats.Stats{ObjectTypes: stats.ObjectTypes{}}}
}

func (s *Stats) CountObjects(ctx context.Context, tx *bolt.Tx) error {
	iter, err := bdb.NewScanIterator[dsc3.Object](ctx, tx, bdb.ObjectsPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		obj := iter.Value()
		s.incObject(obj)
	}

	return nil
}

// relation stats.
func (s *Stats) CountRelations(ctx context.Context, tx *bolt.Tx) error {
	iter, err := bdb.NewScanIterator[dsc3.Relation](ctx, tx, bdb.RelationsObjPath)
	if err != nil {
		return err
	}

	for iter.Next() {
		rel := iter.Value()
		s.incRelation(rel)
	}

	return nil
}

func (s *Stats) incObject(obj *dsc3.Object) {
	ot, ok := s.ObjectTypes[model.ObjectName(obj.Type)]
	if !ok {
		ot.Relations = stats.Relations{}
		s.ObjectTypes[model.ObjectName(obj.Type)] = ot
	}

	atomic.AddInt32(&ot.ObjCount, 1)
}

func (s *Stats) incRelation(rel *dsc3.Relation) {
	objType := model.ObjectName(rel.ObjectType)
	relation := model.RelationName(rel.Relation)
	subType := model.ObjectName(rel.SubjectType)
	subRel := model.RelationName(rel.SubjectRelation)
	if rel.SubjectId == "*" {
		subType += ":*"
	}

	// object_types
	ot, ok := s.ObjectTypes[objType]
	if !ok {
		s.ObjectTypes[objType] = ot
	}
	atomic.AddInt32(&ot.Count, 1)

	if ot.Relations == nil {
		ot.Relations = stats.Relations{}
	}

	// relations
	re, ok := ot.Relations[relation]
	if !ok {
		re.SubjectTypes = stats.SubjectTypes{}
		ot.Relations[relation] = re
	}
	atomic.AddInt32(&re.Count, 1)

	// subject_types
	st, ok := re.SubjectTypes[subType]
	if !ok {
		st.SubjectRelations = stats.SubjectRelations{}
		re.SubjectTypes[subType] = st
	}
	atomic.AddInt32(&st.Count, 1)

	// subject_relations
	if subRel != "" {
		sr, ok := st.SubjectRelations[subRel]
		if !ok {
			st.SubjectRelations[subRel] = sr
		}
		atomic.AddInt32(&sr.Count, 1)
	}
}
