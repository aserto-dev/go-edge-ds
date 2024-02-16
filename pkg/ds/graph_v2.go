package ds

import (
	"context"
	"strings"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/model"
	"github.com/aserto-dev/azm/safe"
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"google.golang.org/protobuf/proto"

	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
)

type getGraphV2 struct {
	*dsr2.GetGraphRequest
}

func GetGraphV2(i *dsr2.GetGraphRequest) *getGraphV2 {
	return &getGraphV2{i}
}

func (i *getGraphV2) Validate(mc *cache.Cache) error {
	if i == nil || i.GetGraphRequest == nil {
		return ErrInvalidRequest.Msg("get_graph")
	}

	// anchor must be defined, hence use an ObjectIdentifier.
	if err := ObjectIdentifierV2(i.GetAnchor()).Validate(mc); err != nil {
		return err
	}

	// Object can be optional, hence the use of an ObjectSelector.
	if err := ObjectSelectorV2(i.GetObject()).Validate(mc); err != nil {
		return err
	}

	// Relation can be optional, hence the use of a RelationTypeSelector.
	if i.GetRelation() != nil {
		objType := i.GetRelation().GetObjectType()
		if objType == "" {
			objType = i.GetObject().GetType()
		}
		if !mc.RelationExists(model.ObjectName(objType), model.RelationName(i.GetRelation().GetName())) {
			return ErrRelationNotFound.Msgf("%s%s%s", i.GetObject().GetType(), RelationSeparator, i.Relation)
		}
	}

	// Subject can be option, hence the use of an ObjectSelector.
	if err := ObjectSelectorV2(i.GetSubject()).Validate(mc); err != nil {
		return err
	}

	// either Object or Subject must be equal to the Anchor to indicate the directionality of the graph walk.
	// Anchor == Subject ==> subject->object (this was the default and only directionality before enabling bi-directionality)
	// Anchor == Object ==> object->subject
	if !ObjectIdentifierV2(i.GetAnchor()).Equal(ObjectIdentifierV2(i.GetObject()).ObjectIdentifier) &&
		!ObjectIdentifierV2(i.GetAnchor()).Equal(ObjectIdentifierV2(i.GetSubject()).ObjectIdentifier) {
		return ErrGraphDirectionality
	}

	return nil
}

func (i *getGraphV2) Exec(ctx context.Context, tx *bolt.Tx /*, resolver *cache.Cache*/) ([]*dsc2.ObjectDependency, error) {
	resp := []*dsc2.ObjectDependency{}

	// determine graph walk directionality.
	// Anchor == Subject ==> subject -> object
	// Anchor == Object ==> object -> subject
	var direction Direction
	if ObjectIdentifierV2(i.GetAnchor()).Equal(ObjectIdentifierV2(i.GetSubject()).ObjectIdentifier) {
		direction = SubjectToObject
	} else if ObjectIdentifierV2(i.GetAnchor()).Equal(ObjectIdentifierV2(i.GetObject()).ObjectIdentifier) {
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

func ObjectIdentifierV2(i *dsc2.ObjectIdentifier) *safe.SafeObjectIdentifier {
	return safe.ObjectIdentifier(&dsc3.ObjectIdentifier{
		ObjectType: i.GetType(),
		ObjectId:   i.GetKey(),
	})
}

func ObjectSelectorV2(s *dsc2.ObjectIdentifier) *safe.SafeObjectSelector {
	return safe.ObjectSelector(&dsc3.ObjectIdentifier{
		ObjectType: s.GetType(),
		ObjectId:   s.GetKey(),
	})
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

func (i *getGraphV2) newGraphWalker(ctx context.Context, tx *bolt.Tx, direction Direction) *GraphWalker {
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

	if err := w.walk(GetGraphV2(w.req).GetAnchor(), 0, []string{}); err != nil {
		return err
	}

	return nil
}

func (w *GraphWalker) Filter() error {
	filters := []func(item *dsc2.ObjectDependency) bool{}

	// SubjectToObject: subject == anchor => filter on object & relation.
	if w.direction == SubjectToObject {
		if w.req.GetObject().GetType() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetObjectType(), w.req.GetObject().GetType())
			})
		}

		if w.req.GetObject().GetKey() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetObjectKey(), w.req.GetObject().GetKey())
			})
		}

		if w.req.GetRelation() != nil {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetRelation(), w.req.GetRelation().GetName())
			})
		}
	}

	// ObjectToSubject: object == anchor => filter on subject & relation.
	if w.direction == ObjectToSubject {
		if w.req.GetSubject().GetType() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetSubjectType(), w.req.GetSubject().GetType())
			})
		}

		if w.req.GetSubject().GetKey() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetSubjectKey(), w.req.GetSubject().GetKey())
			})
		}

		if w.req.GetRelation().GetName() != "" {
			filters = append(filters, func(item *dsc2.ObjectDependency) bool {
				return strings.EqualFold(item.GetRelation(), w.req.GetRelation().GetName())
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

	a := ObjectIdentifierV2(anchor)
	filter := a.GetObjectType() + TypeIDSeparator + a.GetObjectId() + InstanceSeparator

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
			rel.GetSubjectId(),
		)

		dep := dsc2.ObjectDependency{
			ObjectType:  rel.GetObjectType(),
			ObjectKey:   rel.GetObjectId(),
			Relation:    rel.GetRelation(),
			SubjectType: rel.GetSubjectType(),
			SubjectKey:  rel.GetSubjectId(),
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

func (w *GraphWalker) next(r *dsc3.Relation) *dsc2.ObjectIdentifier {
	if w.direction == ObjectToSubject {
		return &dsc2.ObjectIdentifier{Type: proto.String(r.GetSubjectType()), Key: proto.String(r.GetSubjectId())}
	}
	return &dsc2.ObjectIdentifier{Type: proto.String(r.GetObjectType()), Key: proto.String(r.GetObjectId())}
}
