package ds

import (
	"context"
	"strings"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/samber/lo"

	bolt "go.etcd.io/bbolt"
)

type getGraph struct {
	*dsr2.GetGraphRequest
}

func GetGraph(i *dsr2.GetGraphRequest) *getGraph {
	return &getGraph{i}
}

func (i *getGraph) Validate() (bool, error) {
	if i == nil || i.GetGraphRequest == nil {
		return false, ErrInvalidArgumentObjectType.Msg("get graph request not set (nil)")
	}

	// anchor must be defined, hence use an ObjectIdentifier.
	if ok, err := ObjectIdentifier(i.GetGraphRequest.Anchor).Validate(); !ok {
		return ok, err
	}

	// ensure object param block is initialized.
	if i.GetGraphRequest.Object == nil {
		i.GetGraphRequest.Object = &dsc2.ObjectIdentifier{}
	}

	// Object can be optional, hence the use of an ObjectSelector.
	if ok, err := ObjectSelector(i.GetGraphRequest.Object).Validate(); !ok {
		return ok, err
	}

	// ensure the relation param block is initialized.
	if i.GetGraphRequest.Relation == nil {
		i.GetGraphRequest.Relation = &dsc2.RelationTypeIdentifier{}
	}

	// Relation can be optional, hence the use of a RelationTypeSelector.
	if ok, err := RelationTypeSelector(i.GetGraphRequest.Relation).Validate(); !ok {
		return ok, err
	}

	// ensure the subject param block is initialized.
	if i.GetGraphRequest.Subject == nil {
		i.GetGraphRequest.Subject = &dsc2.ObjectIdentifier{}
	}

	// Subject can be option, hence the use of an ObjectSelector.
	if ok, err := ObjectSelector(i.GetGraphRequest.Subject).Validate(); !ok {
		return ok, err
	}

	// either Object or Subject must be equal to the Anchor to indicate the directionality of the graph walk.
	// Anchor == Subject ==> subject->object (this was the default and only directionality before enabling bi-directionality)
	// Anchor == Object ==> object->subject
	if !ObjectIdentifier(i.GetGraphRequest.Anchor).Equal(i.GetGraphRequest.GetObject()) &&
		!ObjectIdentifier(i.GetGraphRequest.Anchor).Equal(i.GetGraphRequest.GetSubject()) {
		return false, ErrGraphDirectionality
	}

	return true, nil
}

func (i *getGraph) Exec(ctx context.Context, tx *bolt.Tx /*, resolver *azm.Model*/) ([]*dsc2.ObjectDependency, error) {
	resp := []*dsc2.ObjectDependency{}

	// determine graph walk directionality.
	// Anchor == Subject ==> subject -> object
	// Anchor == Object ==> object -> subject
	var direction Direction
	if ObjectIdentifier(i.Anchor).Equal(i.Subject) {
		direction = SubjectToObject
	} else if ObjectIdentifier(i.Anchor).Equal(i.Object) {
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
	req        *dsr2.GetGraphRequest
	results    []*dsc2.ObjectDependency
}

func (i *getGraph) newGraphWalker(ctx context.Context, tx *bolt.Tx, direction Direction) *GraphWalker {
	return &GraphWalker{
		ctx:       ctx,
		tx:        tx,
		direction: direction,
		req:       i.GetGraphRequest,
		results:   []*dsc2.ObjectDependency{},
	}
}

func (w *GraphWalker) Fetch() error {
	if w.direction == SubjectToObject {
		w.bucketPath = bdb.RelationsSubPath
	}

	if w.direction == ObjectToSubject {
		w.bucketPath = bdb.RelationsObjPath
	}

	if err := w.walk(w.req.Anchor, 0, []string{}); err != nil {
		return err
	}

	return nil
}

func (w *GraphWalker) Filter() error {
	filters := []func(item *dsc2.ObjectDependency) bool{}

	// SubjectToObject: subject == anchor => filter on object & relation.
	if w.direction == SubjectToObject {
		if w.req.Object.GetType() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetObjectType(), w.req.Object.GetType())
			})
		}

		if w.req.Object.GetKey() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetObjectKey(), w.req.Object.GetKey())
			})
		}

		if w.req.Relation.GetName() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetRelation(), w.req.Relation.GetName())
			})
		}
	}

	// ObjectToSubject: object == anchor => filter on subject & relation.
	if w.direction == ObjectToSubject {
		if w.req.Subject.GetType() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetSubjectType(), w.req.Subject.GetType())
			})
		}

		if w.req.Subject.GetKey() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetSubjectKey(), w.req.Subject.GetKey())
			})
		}

		if w.req.Relation.GetName() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetRelation(), w.req.Relation.GetName())
			})
		}
	}

	w.results = lo.Filter[*dsc2.ObjectDependency](w.results, func(item *dsc2.ObjectDependency, index int) bool {
		for _, filter := range filters {
			if !filter(item) {
				return false
			}
		}
		return true
	})

	return nil
}

func (w *GraphWalker) Results() ([]*dsc2.ObjectDependency, error) {
	return w.results, w.err
}

func (w *GraphWalker) walk(anchor *dsc2.ObjectIdentifier, depth int32, path []string) error {
	depth++

	if depth > maxDepth {
		w.results = []*dsc2.ObjectDependency{}
		w.err = derr.ErrMaxDepthExceeded
		return w.err
	}

	filter := ObjectIdentifier(anchor).Key() + InstanceSeparator

	relations, err := bdb.Scan[dsc2.Relation](w.ctx, w.tx, w.bucketPath, filter)
	if err != nil {
		return err
	}

	for i := 0; i < len(relations); i++ {
		rel := relations[i]

		p := make([]string, len(path))
		copy(p, path)
		p = append(p, rel.GetObject().GetType()+
			TypeIDSeparator+
			rel.GetObject().GetKey()+
			InstanceSeparator+
			rel.GetRelation()+
			InstanceSeparator+
			rel.GetSubject().GetType()+
			TypeIDSeparator+
			rel.GetSubject().GetKey())

		dep := dsc2.ObjectDependency{
			ObjectType:  rel.GetObject().GetType(),
			ObjectKey:   rel.GetObject().GetKey(),
			Relation:    rel.Relation,
			SubjectType: rel.GetSubject().GetType(),
			SubjectKey:  rel.GetObject().GetKey(),
			Depth:       depth,
			IsCycle:     false,
			Path:        p,
		}

		w.results = append(w.results, &dep)

		if err := w.walk(w.next(rel), depth, p); err != nil {
			return err
		}
	}
	return nil
}

func (w *GraphWalker) next(r *dsc2.Relation) *dsc2.ObjectIdentifier {
	if w.direction == ObjectToSubject {
		return r.GetSubject()
	}
	return r.GetObject()
}
