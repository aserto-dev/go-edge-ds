package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-utils/cerr"
)

type objectTypeIdentifier struct{}

var ObjectTypeIdentifier = objectTypeIdentifier{}

func (objectTypeIdentifier) Validate(i *dsc.ObjectTypeIdentifier) (bool, error) {
	if i == nil {
		return false, cerr.ErrInvalidArgument.Msg("object_type_identifier")
	}

	if i.Id != nil && *i.Id > 0 {
		return true, nil
	}
	if i.Name != nil && *i.Name != "" {
		return true, nil
	}

	return false, cerr.ErrInvalidArgument.Msg("object_type_identifier")
}
