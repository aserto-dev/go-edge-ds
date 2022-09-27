package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-utils/cerr"
)

type permissionIdentifier struct{}

var PermissionIdentifier = permissionIdentifier{}

func (permissionIdentifier) Validate(i *dsc.PermissionIdentifier) (bool, error) {
	if i == nil {
		return false, cerr.ErrInvalidArgument.Msg("permission_identifier")
	}

	if i.Id != nil && ID.IsValid(*i.Id) {
		return true, nil
	}
	if i.Name != nil && *i.Name != "" {
		return true, nil
	}

	return false, cerr.ErrInvalidArgument.Msg("permission_identifier")
}
