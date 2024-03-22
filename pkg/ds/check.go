package ds

import (
	"context"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/graph"
	"github.com/aserto-dev/azm/safe"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"

	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
)

type check struct {
	*safe.SafeCheck
}

func Check(i *dsr3.CheckRequest) *check {
	return &check{safe.Check(i)}
}

func (i *check) Exec(ctx context.Context, tx *bolt.Tx, mc *cache.Cache) (*dsr3.CheckResponse, error) {
	return mc.Check(i.CheckRequest, getRelations(ctx, tx))
}

func getRelations(ctx context.Context, tx *bolt.Tx) graph.RelationReader {
	return func(r *dsc3.Relation) ([]*dsc3.Relation, error) {
		path, keyFilter, valueFilter := Relation(r).Filter()
		relations, err := bdb.Scan[dsc3.Relation](ctx, tx, path, keyFilter)
		if err != nil {
			return nil, err
		}

		return lo.Filter(relations, func(r *dsc3.Relation, _ int) bool {
			return valueFilter(r)
		}), nil
	}
}
