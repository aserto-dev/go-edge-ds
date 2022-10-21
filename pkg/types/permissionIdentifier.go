package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type PermissionIdentifier struct {
	*dsc.PermissionIdentifier
}

func NewPermissionIdentifier(i *dsc.PermissionIdentifier) *PermissionIdentifier {
	if i == nil {
		return &PermissionIdentifier{PermissionIdentifier: &dsc.PermissionIdentifier{}}
	}
	return &PermissionIdentifier{PermissionIdentifier: i}
}

func (i *PermissionIdentifier) Msg() *dsc.PermissionIdentifier {
	return i.PermissionIdentifier
}

func (i *PermissionIdentifier) Validate() (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidArgument.Msg("permission_identifier")
	}

	if i.Id != nil && ID.IsValid(*i.Id) {
		return true, nil
	}
	if i.Name != nil && *i.Name != "" {
		return true, nil
	}

	return false, derr.ErrInvalidArgument.Msg("permission_identifier")
}

func (i *PermissionIdentifier) Resolve(sc *StoreContext) (*PermissionIdentifier, error) {
	perm, err := sc.GetPermission(i)
	if err != nil {
		return nil, err
	}

	return &PermissionIdentifier{
		&dsc.PermissionIdentifier{
			Id:   &perm.Id,
			Name: &perm.Name,
		},
	}, nil
}
