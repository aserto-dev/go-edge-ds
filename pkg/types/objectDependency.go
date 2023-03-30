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

type objectDependency struct {
	*dsc.ObjectDependency
}

func ObjectDependency(i *dsc.ObjectDependency) *objectDependency { return &objectDependency{i} }

func (sc *StoreContext) GetGraph(req *dsr.GetGraphRequest) ([]*dsc.ObjectDependency, error) {
	if req == nil {
		return nil, derr.ErrInvalidArgument
	}

	if ok, err := ObjectIdentifier(req.Anchor).Validate(); !ok {
		return []*dsc.ObjectDependency{}, err
	}

	// TODO: validate this will change when supporting non-concrete object instances
	// resolve anchor object, validate object existence
	_, err := sc.GetObject(req.Anchor)
	if err != nil {
		return []*dsc.ObjectDependency{}, err
	}

	deps := []*dsc.ObjectDependency{}
	if deps, err = sc.getObjectDependencies(req.Anchor, 0, []string{}, deps); err != nil {
		return []*dsc.ObjectDependency{}, err
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

// TODO: validate - replace anchorID with *ObjectIdentifier instance.
func (sc *StoreContext) getObjectDependencies(anchor *dsc.ObjectIdentifier, depth int32, path []string, deps []*dsc.ObjectDependency) ([]*dsc.ObjectDependency, error) {
	depth++

	if depth > maxDepth {
		return []*dsc.ObjectDependency{}, derr.ErrMaxDepthExceeded
	}

	// TODO: breaking subFilter must reflect new on-disk structure.
	subFilter := anchor.String() // WAS anchorID + "|"
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
		p = append(p, rel.GetSubject().GetType()+":"+rel.GetSubject().GetKey()+"|"+rel.GetRelation()+"|"+rel.GetObject().GetType()+":"+rel.GetObject().GetKey())

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

		deps = append(deps, &dep)

		if deps, err = sc.getObjectDependencies(rel.GetObject(), depth, p, deps); err != nil {
			return nil, err
		}
	}
	return deps, nil
}

type filter func(*dsc.ObjectDependency) bool

func filterObjectDependencies(req *dsr.GetGraphRequest, deps []*dsc.ObjectDependency) []*dsc.ObjectDependency {
	filters := []filter{}

	if req.Object != nil && req.Object.Type != nil && *req.Object.Type != "" {
		filters = append(filters, func(od *dsc.ObjectDependency) bool {
			return strings.EqualFold(od.ObjectType, req.Object.GetType())
		})
	}
	if req.Object != nil && req.Object.Key != nil {
		filters = append(filters, func(od *dsc.ObjectDependency) bool {
			return od.ObjectKey == req.Object.GetKey()
		})
	}

	if req.Relation != nil && req.Relation.Name != nil && *req.Relation.Name != "" {
		filters = append(filters, func(od *dsc.ObjectDependency) bool {
			return strings.EqualFold(od.Relation, req.Relation.GetName())
		})
	}

	if req.Subject != nil && req.Subject.Type != nil && *req.Subject.Type != "" {
		filters = append(filters, func(od *dsc.ObjectDependency) bool {
			return strings.EqualFold(od.SubjectType, req.Subject.GetType())
		})
	}
	if req.Subject != nil && req.Subject.Key != nil {
		filters = append(filters, func(od *dsc.ObjectDependency) bool {
			return od.SubjectKey == req.Subject.GetKey()
		})
	}

	results := []*dsc.ObjectDependency{}
	for i := 0; i < len(deps); i++ {
		if includeObjectDependency(deps[i], filters) {
			results = append(results, deps[i])
		}
	}

	return results
}

func includeObjectDependency(dep *dsc.ObjectDependency, filters []filter) bool {
	for _, fn := range filters {
		if !fn(dep) {
			return false
		}
	}
	return true
}
