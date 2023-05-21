package ds

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aserto-dev/azm"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type getGraph struct {
	*dsr.GetGraphRequest
}

func GetGraph(i *dsr.GetGraphRequest) *getGraph {
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
		i.GetGraphRequest.Object = &dsc.ObjectIdentifier{}
	}

	// Object can be optional, hence the use of an ObjectSelector.
	if ok, err := ObjectSelector(i.GetGraphRequest.Object).Validate(); !ok {
		return ok, err
	}

	// ensure the relation param block is initialized.
	if i.GetGraphRequest.Relation == nil {
		i.GetGraphRequest.Relation = &dsc.RelationTypeIdentifier{}
	}

	// Relation can be optional, hence the use of a RelationTypeSelector.
	if ok, err := RelationTypeSelector(i.GetGraphRequest.Relation).Validate(); !ok {
		return ok, err
	}

	// ensure the subject param block is initialized.
	if i.GetGraphRequest.Subject == nil {
		i.GetGraphRequest.Subject = &dsc.ObjectIdentifier{}
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

func (i *getGraph) Exec(ctx context.Context, tx *bolt.Tx, resolver *azm.Model) ([]*dsc.ObjectDependency, error) {
	resp := []*dsc.ObjectDependency{}

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

	walker, err := NewGraphWalker(ctx, tx, direction)
	if err != nil {
		return resp, err
	}

	if err := walker.Walk(i.Anchor, 0, []string{}); err != nil {
		return resp, err
	}

	return walker.results, nil
}

type Direction int

const (
	SubjectToObject Direction = 0
	ObjectToSubject Direction = 1
)

type GraphWalker struct {
	ctx        context.Context
	tx         *bolt.Tx
	log        *zerolog.Logger
	bucketPath []string
	direction  Direction
	err        error
	results    []*dsc.ObjectDependency
}

func NewGraphWalker(ctx context.Context, tx *bolt.Tx, direction Direction) (*GraphWalker, error) {
	logFile, _ := os.Create(logFileName())
	logger := zerolog.New(logFile)

	w := &GraphWalker{
		ctx:       ctx,
		tx:        tx,
		log:       &logger,
		direction: direction,
		results:   []*dsc.ObjectDependency{},
	}

	if w.direction == SubjectToObject {
		w.bucketPath = bdb.RelationsSubPath
	}

	if w.direction == ObjectToSubject {
		w.bucketPath = bdb.RelationsObjPath
	}

	return w, nil
}

func (w *GraphWalker) Walk(anchor *dsc.ObjectIdentifier, depth int32, path []string) error {
	depth++

	if depth > maxDepth {
		w.results = []*dsc.ObjectDependency{}
		w.err = derr.ErrMaxDepthExceeded
		return w.err
	}

	filter := ObjectIdentifier(anchor).Key() + InstanceSeparator

	w.log.Debug().Str("filter", filter).Msg("anchor")

	relations, err := bdb.Scan[dsc.Relation](w.ctx, w.tx, w.bucketPath, filter)
	if err != nil {
		return err
	}

	for _, rel := range relations {
		w.log.Debug().
			Int32("depth", depth).
			Str("object_type", rel.GetObject().GetType()).
			Str("object_key", rel.GetObject().GetKey()).
			Str("relation", rel.GetRelation()).
			Str("subject_type", rel.GetSubject().GetType()).
			Str("subject_key", rel.GetSubject().GetKey()).
			Msg("rel")
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

		dep := dsc.ObjectDependency{
			ObjectType:  rel.GetObject().GetType(),
			ObjectKey:   rel.GetObject().GetKey(),
			Relation:    rel.Relation,
			SubjectType: rel.GetSubject().GetType(),
			SubjectKey:  rel.GetObject().GetKey(),
			Depth:       depth,
			IsCycle:     false,
			Path:        p,
		}

		w.log.Debug().
			Int32("depth", depth).
			Str("object_type", rel.GetObject().GetType()).
			Str("object_key", rel.GetObject().GetKey()).
			Str("relation", rel.GetRelation()).
			Str("subject_type", rel.GetSubject().GetType()).
			Str("subject_key", rel.GetSubject().GetKey()).
			Msg("dep")

		w.results = append(w.results, &dep)

		if err := w.Walk(w.next(rel), depth, p); err != nil {
			return err
		}
	}
	return nil
}

func (w *GraphWalker) Results() ([]*dsc.ObjectDependency, error) {
	return w.results, w.err
}

func (w *GraphWalker) next(r *dsc.Relation) *dsc.ObjectIdentifier {
	if w.direction == ObjectToSubject {
		return r.GetSubject()
	}
	return r.GetObject()
}

func logFileName() string {
	return fmt.Sprintf("graph-%s.log", timeStamp())
}

func timeStamp() string {
	ts := time.Now().UTC().Format(time.RFC3339)
	return strings.Replace(strings.Replace(ts, ":", "", -1), "-", "", -1)
}
