package ds

import (
	"context"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/model"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-directory/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
)

type checkPermission struct {
	*dsr3.CheckPermissionRequest
}

func CheckPermission(i *dsr3.CheckPermissionRequest) *checkPermission {
	return &checkPermission{i}
}

func (i *checkPermission) Object() *dsc3.ObjectIdentifier {
	return &dsc3.ObjectIdentifier{
		ObjectType: i.ObjectType,
		ObjectId:   i.ObjectId,
	}
}

func (i *checkPermission) Subject() *dsc3.ObjectIdentifier {
	return &dsc3.ObjectIdentifier{
		ObjectType: i.SubjectType,
		ObjectId:   i.SubjectId,
	}
}

func (i *checkPermission) Validate(mc *cache.Cache) error {
	if i == nil || i.CheckPermissionRequest == nil {
		return ErrInvalidRequest.Msg("check_permission")
	}

	if err := ObjectIdentifier(i.Object()).Validate(mc); err != nil {
		return err
	}

	if err := ObjectIdentifier(i.Subject()).Validate(mc); err != nil {
		return err
	}

	if !mc.PermissionExists(model.ObjectName(i.ObjectType), model.RelationName(i.Permission)) {
		return ErrPermissionNotFound.Msgf("%s%s%s", i.ObjectType, RelationSeparator, i.Permission)
	}

	return nil
}

func (i *checkPermission) Exec(ctx context.Context, tx *bolt.Tx, mc *cache.Cache) (*dsr3.CheckPermissionResponse, error) {
	resp := &dsr3.CheckPermissionResponse{Check: false, Trace: []string{}}

	check, err := i.newChecker(ctx, tx, bdb.RelationsObjPath, mc)
	if err != nil {
		return resp, err
	}

	match, err := check.check(i.Object())

	return &dsr3.CheckPermissionResponse{Check: match}, err
}

func (i *checkPermission) newChecker(ctx context.Context, tx *bolt.Tx, path []string, mc *cache.Cache) (*permissionChecker, error) {
	relations := mc.ExpandPermission(
		model.ObjectName(i.GetObjectType()),
		model.RelationName(i.GetPermission()))

	userSet, err := CreateUserSet(ctx, tx, i.Subject())
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
		trace:   [][]*dsc3.Relation{},
		visited: map[ot]bool{},
	}, nil
}

type permissionChecker struct {
	ctx     context.Context
	tx      *bolt.Tx
	path    []string
	anchor  *checkPermission
	userSet []*dsc3.ObjectIdentifier
	filter  []model.RelationName
	trace   [][]*dsc3.Relation
	visited map[ot]bool
}

func (c *permissionChecker) check(root *dsc3.ObjectIdentifier) (bool, error) {
	// relations associated to object instance.
	filter := ObjectIdentifier(root).Key() + InstanceSeparator
	relations, err := bdb.Scan[dsc3.Relation](c.ctx, c.tx, c.path, filter)
	if err != nil {
		return false, err
	}

	c.visited[ot{root.ObjectType, root.ObjectId}] = true

	for _, r := range relations {
		if c.isMatch(r) {
			return true, nil
		}
	}

	for _, r := range relations {
		if c.isCandidate(r) {
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

func (c *permissionChecker) isMatch(relation *dsc3.Relation) bool {
	return lo.Contains(c.filter, model.RelationName(relation.Relation)) &&
		pb.Contains[*dsc3.ObjectIdentifier](c.userSet, Relation(relation).Subject())
}

func (c *permissionChecker) isCandidate(r *dsc3.Relation) bool {
	return (lo.Contains(c.filter, model.RelationName(r.Relation)) || r.Relation == "parent") &&
		!c.visited[ot{r.SubjectType, r.SubjectId}]
}
