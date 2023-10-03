package ds

import (
	"context"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/model"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"

	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
)

type checkRelation struct {
	*dsr3.CheckRelationRequest
}

func CheckRelation(i *dsr3.CheckRelationRequest) *checkRelation {
	return &checkRelation{i}
}

func (i *checkRelation) Object() *dsc3.ObjectIdentifier {
	return &dsc3.ObjectIdentifier{
		ObjectType: i.ObjectType,
		ObjectId:   i.ObjectId,
	}
}

func (i *checkRelation) Subject() *dsc3.ObjectIdentifier {
	return &dsc3.ObjectIdentifier{
		ObjectType: i.SubjectType,
		ObjectId:   i.SubjectId,
	}
}

func (i *checkRelation) Validate(mc *cache.Cache) (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check relation request not set (nil)")
	}

	if i.CheckRelationRequest == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check relations request not set (nil)")
	}

	if ok, err := ObjectIdentifier(i.Object()).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectIdentifier(i.Subject()).Validate(); !ok {
		return ok, err
	}

	if !mc.RelationExists(model.ObjectName(i.ObjectType), model.RelationName(i.Relation)) {
		return false, ErrRelationNotFound.Msgf("%s%s%s", i.ObjectType, RelationSeparator, i.Relation)
	}

	return true, nil
}

func (i *checkRelation) Exec(ctx context.Context, tx *bolt.Tx, mc *cache.Cache) (*dsr3.CheckRelationResponse, error) {
	resp := &dsr3.CheckRelationResponse{Check: false, Trace: []string{}}

	check, err := i.newChecker(ctx, tx, bdb.RelationsObjPath, mc)
	if err != nil {
		return resp, err
	}

	match, err := check.check(i.Object())

	return &dsr3.CheckRelationResponse{Check: match}, err
}

func (i *checkRelation) newChecker(ctx context.Context, tx *bolt.Tx, path []string, mc *cache.Cache) (*relationChecker, error) {
	relations := mc.ExpandRelation(
		model.ObjectName(i.GetObjectType()),
		model.RelationName(i.GetRelation()))

	userSet, err := CreateUserSet(ctx, tx, i.Subject())
	if err != nil {
		return nil, err
	}

	return &relationChecker{
		ctx:     ctx,
		tx:      tx,
		path:    path,
		anchor:  i,
		userSet: userSet,
		filter:  relations,
		trace:   [][]*dsc3.Relation{},
	}, nil
}

type relationChecker struct {
	ctx     context.Context
	tx      *bolt.Tx
	path    []string
	anchor  *checkRelation
	userSet []*dsc3.ObjectIdentifier
	filter  []model.RelationName
	trace   [][]*dsc3.Relation
}

func (c *relationChecker) check(root *dsc3.ObjectIdentifier) (bool, error) {
	filter := ObjectIdentifier(root).Key() + InstanceSeparator

	// relations associated to object instance.
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

func (c *relationChecker) isMatch(relation *dsc3.Relation) bool {
	if lo.Contains(c.filter, model.RelationName(relation.Relation)) && pb.Contains[*dsc3.ObjectIdentifier](c.userSet, Relation(relation).Subject()) {
		return true
	}
	return false
}
