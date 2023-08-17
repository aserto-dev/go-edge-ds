package ds

import (
	"context"

	"github.com/aserto-dev/azm"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
)

type checkRelation struct {
	*dsr.CheckRelationRequest
}

func CheckRelation(i *dsr.CheckRelationRequest) *checkRelation {
	return &checkRelation{i}
}

func (i *checkRelation) Validate() (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check relation request not set (nil)")
	}

	if i.CheckRelationRequest == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check relations request not set (nil)")
	}

	if ok, err := ObjectIdentifier(i.CheckRelationRequest.Object).Validate(); !ok {
		return ok, err
	}

	if ok, err := RelationTypeIdentifier(i.CheckRelationRequest.Relation).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectIdentifier(i.CheckRelationRequest.Subject).Validate(); !ok {
		return ok, err
	}

	return true, nil
}

func (i *checkRelation) Exec(ctx context.Context, tx *bolt.Tx, mc *azm.Model) (*dsr.CheckRelationResponse, error) {
	resp := &dsr.CheckRelationResponse{Check: false, Trace: []string{}}

	check, err := i.newChecker(ctx, tx, bdb.RelationsObjPath, mc)
	if err != nil {
		return resp, err
	}

	match, err := check.check(i.Object)

	return &dsr.CheckRelationResponse{Check: match}, err
}

func (i *checkRelation) newChecker(ctx context.Context, tx *bolt.Tx, path []string, mc *azm.Model) (*relationChecker, error) {
	relations := mc.ExpandRelation(i.CheckRelationRequest.Object.GetType(), i.CheckRelationRequest.Relation.GetName())

	userSet, err := CreateUserSet(ctx, tx, i.Subject)
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
		trace:   [][]*dsc.Relation{},
	}, nil
}

type relationChecker struct {
	ctx     context.Context
	tx      *bolt.Tx
	path    []string
	anchor  *checkRelation
	userSet []*dsc.ObjectIdentifier
	filter  []string
	trace   [][]*dsc.Relation
}

func (c *relationChecker) check(root *dsc.ObjectIdentifier) (bool, error) {
	filter := ObjectIdentifier(root).Key() + InstanceSeparator

	// relations associated to object instance.
	relations, err := bdb.Scan[dsc.Relation](c.ctx, c.tx, c.path, filter)
	if err != nil {
		return false, err
	}

	for _, r := range relations {
		if c.isMatch(r) {
			return true, nil
		}
	}

	for _, r := range relations {
		if lo.Contains(c.filter, r.Relation) {
			match, err := c.check(r.Subject)
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

func (c *relationChecker) isMatch(relation *dsc.Relation) bool {
	if lo.Contains(c.filter, relation.Relation) && pb.Contains[*dsc.ObjectIdentifier](c.userSet, relation.Subject) {
		return true
	}
	return false
}
