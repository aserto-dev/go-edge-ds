package ds

import (
	"context"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"

	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
)

type checkRelation struct {
	*dsr2.CheckRelationRequest
}

func CheckRelation(i *dsr2.CheckRelationRequest) *checkRelation {
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

func (i *checkRelation) Exec(ctx context.Context, tx *bolt.Tx) (*dsr2.CheckRelationResponse, error) {
	resp := &dsr2.CheckRelationResponse{Check: false, Trace: []string{}}

	check, err := i.newChecker(ctx, tx, bdb.RelationsObjPath)
	if err != nil {
		return resp, err
	}

	match, err := check.check(i.Object)

	return &dsr2.CheckRelationResponse{Check: match}, err
}

func (i *checkRelation) newChecker(ctx context.Context, tx *bolt.Tx, path []string) (*relationChecker, error) {
	relations, err := ResolveRelation(ctx, tx, i.CheckRelationRequest.Object.GetType(), i.CheckRelationRequest.Relation.GetName())
	if err != nil {
		return nil, err
	}

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
		trace:   [][]*dsc2.Relation{},
	}, nil
}

type relationChecker struct {
	ctx     context.Context
	tx      *bolt.Tx
	path    []string
	anchor  *checkRelation
	userSet []*dsc2.ObjectIdentifier
	filter  []string
	trace   [][]*dsc2.Relation
}

func (c *relationChecker) check(root *dsc2.ObjectIdentifier) (bool, error) {
	filter := ObjectIdentifier(root).Key() + InstanceSeparator

	// relations associated to object instance.
	relations, err := bdb.Scan[dsc2.Relation](c.ctx, c.tx, c.path, filter)
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

func (c *relationChecker) isMatch(relation *dsc2.Relation) bool {
	if lo.Contains(c.filter, relation.Relation) && pb.Contains[*dsc2.ObjectIdentifier](c.userSet, relation.Subject) {
		return true
	}
	return false
}
