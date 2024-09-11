package ds

import (
	"context"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/graph"
	"github.com/aserto-dev/azm/safe"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"

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

func (i *check) RelationIdentifiersExist(ctx context.Context, tx *bolt.Tx) error {
	if exists := i.relationIdentifierExist(
		ctx, tx, bdb.RelationsSubPath,
		ObjectIdentifier(&dsc3.ObjectIdentifier{ObjectType: i.SubjectType, ObjectId: i.SubjectId}).Key(),
	); !exists {
		return derr.ErrObjectNotFound.Msgf("subject %s:%s", i.SubjectType, i.SubjectId)
	}

	if exists := i.relationIdentifierExist(
		ctx, tx, bdb.RelationsObjPath,
		ObjectIdentifier(&dsc3.ObjectIdentifier{ObjectType: i.ObjectType, ObjectId: i.ObjectId}).Key(),
	); !exists {
		return derr.ErrObjectNotFound.Msgf("object %s:%s", i.ObjectType, i.ObjectId)
	}

	return nil
}

func (i *check) relationIdentifierExist(ctx context.Context, tx *bolt.Tx, path bdb.Path, keyFilter string) bool {
	scan, err := bdb.NewScanIterator[dsc3.Relation](ctx, tx, path, bdb.WithPageSize(1), bdb.WithKeyFilter(keyFilter))
	if err != nil {
		return false
	}

	if scan.Next() {
		return true
	}

	return false
}
