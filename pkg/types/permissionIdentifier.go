package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type permissionIdentifier struct {
	*dsc.PermissionIdentifier
}

func PermissionIdentifier(i *dsc.PermissionIdentifier) *permissionIdentifier {
	return &permissionIdentifier{i}
}

func (i *permissionIdentifier) Validate() (bool, error) {
	if i.PermissionIdentifier == nil {
		return false, derr.ErrInvalidArgument.Msg("permission_identifier")
	}

	if i.Name != nil && *i.Name != "" {
		return true, nil
	}

	return false, derr.ErrInvalidArgument.Msg("permission_identifier")
}

func (i *permissionIdentifier) Resolve(sc *StoreContext) (*dsc.PermissionIdentifier, error) {
	perm, err := sc.GetPermission(i.PermissionIdentifier)
	if err != nil {
		return nil, err
	}

	return &dsc.PermissionIdentifier{
		Name: &perm.Name,
	}, nil
}
