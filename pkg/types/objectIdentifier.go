package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type objectIdentifier struct{}

var ObjectIdentifier = objectIdentifier{}

func (objectIdentifier) Validate(i *dsc.ObjectIdentifier) (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidArgument.Msg("object_identifier")
	}
	if i.Id != nil && ID.IsValid(*i.Id) {
		return true, nil
	}
	if i.Key != nil && i.Type != nil &&
		*i.Key != "" && *i.Type != "" {
		return true, nil
	}
	if i.Type != nil && *i.Type != "" {
		return true, nil
	}
	return false, derr.ErrInvalidArgument.Msg("object_identifier")
}
