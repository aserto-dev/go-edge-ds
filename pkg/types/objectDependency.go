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
	if deps, err = sc.getObjectDependencies(anchor.Id, 0, []string{}, deps); err != nil {
		return []*ObjectDependency{}, err
	}

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

func (sc *StoreContext) getObjectDependencies(anchorID string, depth int32, path []string, deps []*ObjectDependency) ([]*ObjectDependency, error) {
	depth++

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

		p := make([]string, len(path))
		copy(p, path)
		p = append(p, *rel.Subject.Type+"|"+*rel.Subject.Id+"|"+rel.Relation+"|"+*rel.Object.Type+"|"+*rel.Object.Id)

		dep := ObjectDependency{
			ObjectDependency: &dsc.ObjectDependency{
				ObjectType:  *rel.Object.Type,
				ObjectId:    *rel.Object.Id,
				ObjectKey:   "",
				Relation:    rel.Relation,
				SubjectType: *rel.Subject.Type,
				SubjectId:   *rel.Subject.Id,
				SubjectKey:  "",
				Depth:       depth,
				IsCycle:     false,
				Path:        p,
			},
		}

		deps = append(deps, &dep)

		if deps, err = sc.getObjectDependencies(*rel.Object.Id, depth, p, deps); err != nil {
			return nil, err
		}
	}
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
