package types

import (
	"bytes"
	"context"
	"sort"
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/v2"

	"github.com/aserto-dev/edge-ds/pkg/boltdb"
	"github.com/aserto-dev/edge-ds/pkg/pb"
)

type ObjectDependency struct {
	*dsc.ObjectDependency
}

type ObjectDependencies []*ObjectDependency

func GetGraph(ctx context.Context, i *dsr.GetGraphRequest, store *boltdb.BoltDB, opts ...boltdb.Opts) (ObjectDependencies, error) {
	if ok, err := ObjectIdentifier.Validate(i.Anchor); !ok {
		return []*ObjectDependency{}, err
	}

	sc := StoreContext{
		Context: ctx,
		Store:   store,
		Opts:    opts,
	}

	// resolve anchor object
	anchor, err := GetObject(ctx, i.Anchor, store, opts...)
	if err != nil {
		return []*ObjectDependency{}, err
	}

	deps := []*ObjectDependency{}
	if deps, err = sc.getObjectDependencies(anchor.Id, 0, []string{anchor.Id}, deps); err != nil {
		return []*ObjectDependency{}, err
	}

	sort.Slice(deps, func(i, j int) bool {
		if deps[i].Depth < deps[j].Depth {
			return true
		}
		if deps[i].Depth == deps[j].Depth && strings.Compare(deps[i].Path, deps[j].Path) == -1 {
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
		p = append(p, *rel.Object.Id)

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
				Path:        strings.Join(p, ","),
			},
		}

		deps = append(deps, &dep)

		if deps, err = sc.getObjectDependencies(*rel.Object.Id, depth, p, deps); err != nil {
			return nil, err
		}
	}
	return deps, nil
}
