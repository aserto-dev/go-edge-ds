package types

import (
	"bytes"
	"sort"
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
)

const maxDepth = 1024

type ObjectDependency struct {
	*dsc.ObjectDependency
}

type ObjectDependencies []*ObjectDependency

func (sc *StoreContext) GetGraph(req *dsr.GetGraphRequest) (ObjectDependencies, error) {
	if req == nil {
		return nil, derr.ErrInvalidArgument
	}

	anchorIdentifier := &ObjectIdentifier{req.Anchor}
	if ok, err := anchorIdentifier.Validate(); !ok {
		return []*ObjectDependency{}, err
	}

	// resolve anchor object
	anchor, err := sc.GetObject(anchorIdentifier)
	if err != nil {
		return []*ObjectDependency{}, err
	}

	deps := []*ObjectDependency{}

	rels := []*Relation{}
	if rels, err = sc.getObjectDependencies2(anchor.Id, rels); err != nil {
		return []*ObjectDependency{}, err
	}
	rels = rewriteParentRelations(rels)

	deps = convertRelationsToDependencies(rels)
	deps = filterObjectDependencies(req, deps)

	sort.Slice(deps, func(i, j int) bool {
		if deps[i].Depth < deps[j].Depth {
			return true
		}
		if (deps[i].Depth == deps[j].Depth) && (strings.Join(deps[i].Path, ",") < strings.Join(deps[j].Path, ",")) {
			return true
		}
		return false
	})

	return deps, nil
}

type filter func(*ObjectDependency) bool

func filterObjectDependencies(req *dsr.GetGraphRequest, deps ObjectDependencies) ObjectDependencies {
	filters := []filter{}

	if req.Object != nil && req.Object.Id != nil && ID.IsValidIfSet(req.Object.GetId()) {
		filters = append(filters, func(od *ObjectDependency) bool {
			return od.ObjectId == req.Object.GetId()
		})
	}
	if req.Object != nil && req.Object.Type != nil && *req.Object.Type != "" {
		filters = append(filters, func(od *ObjectDependency) bool {
			return strings.EqualFold(od.ObjectType, req.Object.GetType())
		})
	}

	if req.Subject != nil && req.Subject.Id != nil && ID.IsValidIfSet(req.Subject.GetId()) {
		filters = append(filters, func(od *ObjectDependency) bool {
			return od.SubjectId == req.Subject.GetId()
		})
	}
	if req.Subject != nil && req.Subject.Type != nil && *req.Subject.Type != "" {
		filters = append(filters, func(od *ObjectDependency) bool {
			return strings.EqualFold(od.SubjectType, req.Subject.GetType())
		})
	}

	if req.Relation != nil && req.Relation.Name != nil && *req.Relation.Name != "" {
		filters = append(filters, func(od *ObjectDependency) bool {
			return strings.EqualFold(od.Relation, req.Relation.GetName())
		})
	}

	results := []*ObjectDependency{}
	for i := 0; i < len(deps); i++ {
		if includeObjectDependency(deps[i], filters) {
			results = append(results, deps[i])
		}
	}

	return results
}

func includeObjectDependency(dep *ObjectDependency, filters []filter) bool {
	for _, fn := range filters {
		if !fn(dep) {
			return false
		}
	}
	return true
}

func (sc *StoreContext) getObjectDependencies2(anchorID string, deps []*Relation) ([]*Relation, error) {

	if len(deps)+1 > maxDepth {
		return []*Relation{}, derr.ErrMaxDepthExceeded
	}

	subFilter := anchorID + "|"
	_, values, err := sc.Store.ReadScan(RelationsSubPath(), subFilter, sc.Opts)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(values); i++ {
		var rel dsc.Relation
		if err := pb.BufToProto(bytes.NewReader(values[i]), &rel); err != nil {
			return nil, err
		}

		deps = append(deps, &Relation{&rel})

		if deps, err = sc.getObjectDependencies2(*rel.Object.Id, deps); err != nil {
			return nil, err
		}
	}
	return deps, nil
}

const parentRelation string = "parent"

func rewriteParentRelations(rels []*Relation) []*Relation {
	var lastRelation string
	for i := 0; i < len(rels); i++ {
		if rels[i].GetRelation() == parentRelation {
			rels[i].Relation.Relation = lastRelation
			continue
		}
		lastRelation = rels[i].Relation.GetRelation()
	}
	return rels
}

func convertRelationsToDependencies(rels []*Relation) ObjectDependencies {
	results := []*ObjectDependency{}
	path := []string{}

	for i := 0; i < len(rels); i++ {
		path = append(path, rels[i].GetSubject().GetType()+"|"+rels[i].GetSubject().GetKey()+"|"+rels[i].GetRelation()+"|"+rels[i].GetObject().GetType()+"|"+rels[i].GetObject().GetKey())

		results = append(results, &ObjectDependency{
			&dsc.ObjectDependency{
				SubjectId:   rels[i].GetSubject().GetId(),
				SubjectType: rels[i].GetSubject().GetType(),
				SubjectKey:  rels[i].GetSubject().GetKey(),
				Relation:    rels[i].GetRelation(),
				ObjectId:    rels[i].GetObject().GetId(),
				ObjectType:  rels[i].GetObject().GetType(),
				ObjectKey:   rels[i].GetObject().GetKey(),
				Depth:       int32(i + 1),
				IsCycle:     false,
				Path:        path,
			},
		})
	}

	return results
}
