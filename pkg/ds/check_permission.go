package ds

import (
	"context"
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/samber/lo"

	bolt "go.etcd.io/bbolt"
)

type checkPermission struct {
	*dsr.CheckPermissionRequest
}

func CheckPermission(i *dsr.CheckPermissionRequest) *checkPermission {
	return &checkPermission{i}
}

func (i *checkPermission) Validate() (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check permission request not set (nil)")
	}

	if i.CheckPermissionRequest == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check permission request not set (nil)")
	}

	if ok, err := ObjectIdentifier(i.CheckPermissionRequest.Object).Validate(); !ok {
		return ok, err
	}

	if ok, err := PermissionIdentifier(i.CheckPermissionRequest.Permission).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectIdentifier(i.CheckPermissionRequest.Subject).Validate(); !ok {
		return ok, err
	}

	return true, nil
}

func (i *checkPermission) Exec(ctx context.Context, tx *bolt.Tx) (*dsr.CheckPermissionResponse, error) {
	resp := &dsr.CheckPermissionResponse{Check: false, Trace: []string{}}

	relations, err := ResolvePermission(ctx, tx, i.CheckPermissionRequest.Object.GetType(), i.CheckPermissionRequest.Permission.GetName())
	if err != nil {
		return resp, err
	}

	check := i.newChecker(ctx, tx, bdb.RelationsObjPath, relations)
	match, err := check.Check(i.Object)

	return &dsr.CheckPermissionResponse{Check: match}, err
}

func (i *checkPermission) newChecker(ctx context.Context, tx *bolt.Tx, path, filter []string) *permissionChecker {
	return &permissionChecker{
		ctx:    ctx,
		tx:     tx,
		path:   path,
		anchor: i,
		filter: filter,
		trace:  [][]*dsc.Relation{},
	}
}

type permissionChecker struct {
	ctx    context.Context
	tx     *bolt.Tx
	path   []string
	anchor *checkPermission
	filter []string
	trace  [][]*dsc.Relation
}

func (c *permissionChecker) Check(root *dsc.ObjectIdentifier) (bool, error) {
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
		if lo.Contains(c.filter, r.Relation) || r.Relation == "parent" {
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

func (c *permissionChecker) isMatch(relation *dsc.Relation) bool {
	if lo.Contains(c.filter, relation.Relation) &&
		strings.EqualFold(relation.Subject.GetType(), c.anchor.Subject.GetType()) &&
		strings.EqualFold(relation.Subject.GetKey(), c.anchor.Subject.GetKey()) {
		return true
	}
	return false
}
