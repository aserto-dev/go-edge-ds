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
	"github.com/aserto-dev/go-directory/pkg/prop"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/types/known/structpb"
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

		return bdb.ScanWithFilter[dsc3.Relation](ctx, tx, path, keyFilter, valueFilter)
	}
}

func (i *check) RelationIdentifiersExist(ctx context.Context, tx *bolt.Tx) error {
	if !i.relationIdentifierExist(
		ctx, tx, bdb.RelationsSubPath,
		ObjectIdentifier(&dsc3.ObjectIdentifier{ObjectType: i.SubjectType, ObjectId: i.SubjectId}).Key(),
	) {
		return derr.ErrObjectNotFound.Msgf("subject %s:%s", i.SubjectType, i.SubjectId)
	}

	if !i.relationIdentifierExist(
		ctx, tx, bdb.RelationsObjPath,
		ObjectIdentifier(&dsc3.ObjectIdentifier{ObjectType: i.ObjectType, ObjectId: i.ObjectId}).Key(),
	) {
		return derr.ErrObjectNotFound.Msgf("object %s:%s", i.ObjectType, i.ObjectId)
	}

	return nil
}

func (i *check) relationIdentifierExist(ctx context.Context, tx *bolt.Tx, path bdb.Path, keyFilter string) bool {
	exists, err := bdb.KeyPrefixExists[dsc3.Relation](ctx, tx, path, keyFilter)
	if err != nil {
		return false
	}
	return exists
}

func SetContextWithReason(err error) *structpb.Struct {
	return &structpb.Struct{
		Fields: map[string]*structpb.Value{
			prop.Reason: structpb.NewStringValue(err.Error()),
		},
	}
}
