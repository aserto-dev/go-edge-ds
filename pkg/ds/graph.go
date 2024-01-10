package ds

import (
	"context"
	"strings"

	"github.com/aserto-dev/azm/safe"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
)

type getGraph struct {
	*safe.SafeGetGraph
}

func GetGraph(i *dsr3.GetGraphRequest) *getGraph {
	return &getGraph{safe.GetGraph(i)}
}

func (i *getGraph) Exec(ctx context.Context, tx *bolt.Tx /*, resolver *cache.Cache*/) ([]*dsc3.ObjectDependency, error) {
	resp := []*dsc3.ObjectDependency{}

	// determine graph walk directionality.
	// Anchor == Subject ==> subject -> object
	// Anchor == Object ==> object -> subject
	var direction Direction
	if ObjectIdentifier(i.Anchor()).Equal(i.Subject()) {
		direction = SubjectToObject
	} else if ObjectIdentifier(i.Anchor()).Equal(i.Object()) {
		direction = ObjectToSubject
	} else {
		return resp, ErrGraphDirectionality
	}

	walker := i.newGraphWalker(ctx, tx, direction)

	if err := walker.Fetch(); err != nil {
		return resp, err
	}

	if err := walker.Filter(); err != nil {
		return resp, err
	}

	return walker.Results()
}

type Direction int

const (
	SubjectToObject Direction = 0
	ObjectToSubject Direction = 1
)

type GraphWalker struct {
	ctx        context.Context
	tx         *bolt.Tx
	bucketPath []string
	direction  Direction
	err        error
	req        *dsr3.GetGraphRequest
	results    []*dsc3.ObjectDependency
}

func (i *getGraph) newGraphWalker(ctx context.Context, tx *bolt.Tx, direction Direction) *GraphWalker {
	return &GraphWalker{
		ctx:       ctx,
		tx:        tx,
		direction: direction,
		req:       i.GetGraphRequest,
		results:   []*dsc3.ObjectDependency{},
	}
}

func (w *GraphWalker) Fetch() error {
	if w.direction == SubjectToObject {
		w.bucketPath = bdb.RelationsSubPath
	}

	if w.direction == ObjectToSubject {
		w.bucketPath = bdb.RelationsObjPath
	}

	if err := w.walk(GetGraph(w.req).Anchor(), 0, []string{}); err != nil {
		return err
	}

	return nil
}

func (w *GraphWalker) Filter() error {
	filters := []func(item *dsc3.ObjectDependency) bool{}

	// SubjectToObject: subject == anchor => filter on object & relation.
	if w.direction == SubjectToObject {
		if w.req.GetObjectType() != "" {
			filters = append(filters, func(item *dsc3.ObjectDependency) bool {
				return strings.EqualFold(item.GetObjectType(), w.req.GetObjectType())
			})
		}

		if w.req.GetObjectId() != "" {
			filters = append(filters, func(item *dsc3.ObjectDependency) bool {
				return strings.EqualFold(item.GetObjectId(), w.req.GetObjectId())
			})
		}

		if w.req.GetRelation() != "" {
			filters = append(filters, func(item *dsc3.ObjectDependency) bool {
				return strings.EqualFold(item.GetRelation(), w.req.GetRelation())
			})
		}
	}

	// ObjectToSubject: object == anchor => filter on subject & relation.
	if w.direction == ObjectToSubject {
		if w.req.GetSubjectType() != "" {
			filters = append(filters, func(item *dsc3.ObjectDependency) bool {
				return strings.EqualFold(item.GetSubjectType(), w.req.GetSubjectType())
			})
		}

		if w.req.GetSubjectId() != "" {
			filters = append(filters, func(item *dsc3.ObjectDependency) bool {
				return strings.EqualFold(item.GetSubjectId(), w.req.GetSubjectId())
			})
		}

		if w.req.GetRelation() != "" {
			filters = append(filters, func(item *dsc3.ObjectDependency) bool {
				return strings.EqualFold(item.GetRelation(), w.req.GetRelation())
			})
		}
	}

	w.results = lo.Filter[*dsc3.ObjectDependency](w.results, func(item *dsc3.ObjectDependency, index int) bool {
		for _, filter := range filters {
			if !filter(item) {
				return false
			}
		}
		return true
	})

	return nil
}

func (w *GraphWalker) Results() ([]*dsc3.ObjectDependency, error) {
	return w.results, w.err
}

func (w *GraphWalker) walk(anchor *dsc3.ObjectIdentifier, depth int32, path []string) error {
	depth++

	if depth > maxDepth {
		w.results = []*dsc3.ObjectDependency{}
		w.err = derr.ErrMaxDepthExceeded
		return w.err
	}

	filter := ObjectIdentifier(anchor).Key() + InstanceSeparator

	relations, err := bdb.Scan[dsc3.Relation](w.ctx, w.tx, w.bucketPath, filter)
	if err != nil {
		return err
	}

	for i := 0; i < len(relations); i++ {
		rel := relations[i]

		p := make([]string, len(path))
		copy(p, path)
		p = append(p, rel.GetObjectType()+
			TypeIDSeparator+
			rel.GetObjectId()+
			InstanceSeparator+
			rel.GetRelation()+
			InstanceSeparator+
			rel.GetSubjectType()+
			TypeIDSeparator+
			rel.GetSubjectId()+
			RelationSeparator+
			rel.GetSubjectRelation(),
		)

		dep := dsc3.ObjectDependency{
			ObjectType:      rel.GetObjectType(),
			ObjectId:        rel.GetObjectId(),
			Relation:        rel.GetRelation(),
			SubjectType:     rel.GetSubjectType(),
			SubjectId:       rel.GetSubjectId(),
			SubjectRelation: rel.GetSubjectRelation(),
			Depth:           depth,
			IsCycle:         false,
			Path:            p,
		}

		w.results = append(w.results, &dep)

		if err := w.walk(w.next(rel), depth, p); err != nil {
			return err
		}
	}
	return nil
}

func (w *GraphWalker) next(r *dsc3.Relation) *dsc3.ObjectIdentifier {
	if w.direction == ObjectToSubject {
		return Relation(r).Subject()
	}
	return Relation(r).Object()
}
