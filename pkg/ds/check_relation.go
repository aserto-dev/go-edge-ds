package ds

import (
	"context"
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
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

func (i *checkRelation) Exec(ctx context.Context, tx *bolt.Tx) (*dsr.CheckRelationResponse, error) {
	resp := &dsr.CheckRelationResponse{Check: false, Trace: []string{}}

	filter, err := ResolveRelation(ctx, tx, i.CheckRelationRequest.Object.GetType(), i.CheckRelationRequest.Relation.GetName())
	if err != nil {
		return resp, err
	}

	check := i.newChecker(ctx, tx, bdb.RelationsObjPath, filter)
	match, err := check.Check(i.Object)

	return &dsr.CheckRelationResponse{Check: match}, err
}

func (i *checkRelation) newChecker(ctx context.Context, tx *bolt.Tx, path, filter []string) *relationChecker {
	return &relationChecker{
		ctx:    ctx,
		tx:     tx,
		path:   path,
		anchor: i,
		filter: filter,
		trace:  [][]*dsc.Relation{},
	}
}

type relationChecker struct {
	ctx    context.Context
	tx     *bolt.Tx
	path   []string
	anchor *checkRelation
	filter []string
	trace  [][]*dsc.Relation
}

func (c *relationChecker) Check(root *dsc.ObjectIdentifier) (bool, error) {
	filter := ObjectIdentifier(root).Key() + InstanceSeparator
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
		if inSet(c.filter, r.Relation) {
			match, err := c.Check(r.Subject)
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
	if inSet(c.filter, relation.Relation) &&
		strings.EqualFold(relation.Subject.GetType(), c.anchor.Subject.GetType()) &&
		strings.EqualFold(relation.Subject.GetKey(), c.anchor.Subject.GetKey()) {
		return true
	}
	return false
}
