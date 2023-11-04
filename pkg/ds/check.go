package ds

import (
	"context"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/model"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"

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
	resp := &dsr3.CheckResponse{Check: false, Trace: []string{}}

	check, err := i.newChecker(ctx, tx, bdb.RelationsObjPath, mc)
	if err != nil {
		return resp, err
	}

	match, err := check.check(i.Object())

	return &dsr3.CheckResponse{Check: match}, err
}

func (i *check) newChecker(ctx context.Context, tx *bolt.Tx, path []string, mc *cache.Cache) (*checker, error) {
	var relations []model.RelationName
	if mc.PermissionExists(model.ObjectName(i.GetObjectType()), model.PermissionName(i.GetRelation())) {
		relations = mc.ExpandPermission(
			model.ObjectName(i.GetObjectType()),
			model.PermissionName(i.GetRelation()))
	} else if mc.RelationExists(model.ObjectName(i.GetObjectType()), model.RelationName(i.GetRelation())) {
		relations = mc.ExpandRelation(
			model.ObjectName(i.GetObjectType()),
			model.RelationName(i.GetRelation()))
	} else {
		return nil, derr.ErrRelationTypeNotFound.Msg(i.GetRelation())
	}

	userSet, err := CreateUserSet(ctx, tx, i.Subject())
	if err != nil {
		return nil, err
	}

	checker := &checker{
		ctx:     ctx,
		tx:      tx,
		path:    path,
		anchor:  i,
		userSet: userSet,
		filter:  relations,
		trace:   [][]*dsc3.Relation{},
		mc:      mc,
	}

	return checker, nil
}

type checker struct {
	ctx     context.Context
	tx      *bolt.Tx
	path    []string
	anchor  *check
	userSet []*dsc3.ObjectIdentifier
	filter  []model.RelationName
	trace   [][]*dsc3.Relation
	mc      *cache.Cache
}

func (c *checker) check(root *dsc3.ObjectIdentifier) (bool, error) {
	// relations associated to object instance.
	filter := ObjectIdentifier(root).Key() + InstanceSeparator
	relations, err := bdb.Scan[dsc3.Relation](c.ctx, c.tx, c.path, filter)
	if err != nil {
		return false, err
	}

	for _, r := range relations {
		if c.isMatch(r) {
			return true, nil
		}
	}

	for _, r := range relations {
		if lo.Contains(c.filter, model.RelationName(r.Relation)) {
			match, err := c.check(Relation(r).Subject())
			if err != nil {
				return false, err
			}

			if match {
				return match, nil
			}
		}
	}

	return false, nil
}

func (c *checker) isMatch(relation *dsc3.Relation) bool {
	if lo.Contains(c.filter, model.RelationName(relation.Relation)) && pb.Contains[*dsc3.ObjectIdentifier](c.userSet, Relation(relation).Subject()) {
		return true
	}
	return false
}
