package ds

import (
	"context"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/model"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"

	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
)

type check struct {
	*dsr3.CheckRequest
}

func Check(i *dsr3.CheckRequest) *check {
	return &check{i}
}

func (i *check) Object() *dsc3.ObjectIdentifier {
	return &dsc3.ObjectIdentifier{
		ObjectType: i.ObjectType,
		ObjectId:   i.ObjectId,
	}
}

func (i *check) Subject() *dsc3.ObjectIdentifier {
	return &dsc3.ObjectIdentifier{
		ObjectType: i.SubjectType,
		ObjectId:   i.SubjectId,
	}
}

func (i *check) Validate(mc *cache.Cache) (bool, error) {
	if i == nil || i.CheckRequest == nil {
		return false, ErrInvalidRequest.Msg("check")
	}

	if !mc.ObjectExists(model.ObjectName(i.ObjectType)) {
		return false, ErrObjectNotFound.Msgf("object_type: %s", i.ObjectType)
	}

	if !mc.ObjectExists(model.ObjectName(i.SubjectType)) {
		return false, ErrObjectNotFound.Msgf("subject_type: %s", i.SubjectType)
	}

	if !mc.RelationExists(model.ObjectName(i.ObjectType), model.RelationName(i.Relation)) {
		return false, ErrRelationNotFound.Msgf("relation: %s%s%s", i.ObjectType, RelationSeparator, i.Relation)
	}

	return true, nil
}

func (i *check) Exec(ctx context.Context, tx *bolt.Tx, mc *cache.Cache) (*dsr3.CheckResponse, error) {
	return mc.Check(i.CheckRequest, func(r *dsc3.Relation) ([]*dsc3.Relation, error) {
		path, keyFilter, valueFilter := Relation(r).Filter()
		relations, err := bdb.Scan[dsc3.Relation](ctx, tx, path, keyFilter)
		if err != nil {
			return nil, err
		}

		return lo.Filter(relations, func(r *dsc3.Relation, _ int) bool {
			return valueFilter(r)
		}), nil

	})
}
