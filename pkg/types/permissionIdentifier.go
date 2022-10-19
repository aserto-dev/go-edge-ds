package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type PermissionIdentifier struct {
	*dsc.PermissionIdentifier
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
