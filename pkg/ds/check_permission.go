package ds

import (
	"context"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
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

func (i *checkPermission) Exec(ctx context.Context, tx *bolt.Tx, mc *bdb.ModelCache) (*dsr.CheckPermissionResponse, error) {
	resp := &dsr.CheckPermissionResponse{Check: false, Trace: []string{}}

	check, err := i.newChecker(ctx, tx, bdb.RelationsObjPath, mc)
	if err != nil {
		return resp, err
	}

	match, err := check.check(i.Object)

	return &dsr.CheckPermissionResponse{Check: match}, err
}

func (i *checkPermission) newChecker(ctx context.Context, tx *bolt.Tx, path []string, mc *bdb.ModelCache) (*permissionChecker, error) {
	relations := mc.ExpandPermission(i.CheckPermissionRequest.Object.GetType(), i.CheckPermissionRequest.Permission.GetName())

	userSet, err := CreateUserSet(ctx, tx, i.Subject)
	if err != nil {
		return nil, err
	}

	return &permissionChecker{
		ctx:     ctx,
		tx:      tx,
		path:    path,
		anchor:  i,
		userSet: userSet,
		filter:  relations,
		trace:   [][]*dsc.Relation{},
	}, nil
}

type permissionChecker struct {
	ctx     context.Context
	tx      *bolt.Tx
	path    []string
	anchor  *checkPermission
	userSet []*dsc.ObjectIdentifier
	filter  []string
	trace   [][]*dsc.Relation
}

func (c *permissionChecker) check(root *dsc.ObjectIdentifier) (bool, error) {
	// relations associated to object instance.
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

func (c *permissionChecker) isMatch(relation *dsc.Relation) bool {
	if lo.Contains(c.filter, relation.Relation) && pb.Contains[*dsc.ObjectIdentifier](c.userSet, relation.Subject) {
		return true
	}
	return false
}
