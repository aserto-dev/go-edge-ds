package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type ObjectIdentifier struct {
	*dsc.ObjectIdentifier
}

func (i *ObjectIdentifier) Validate() (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidArgument.Msg("object_identifier")
	}
	if ID.IsValid(i.GetId()) {
		return true, nil
	}
	if i.GetKey() != "" && i.GetType() != "" {
		return true, nil
	}
	if i.GetType() != "" {
		return true, nil
	}
	return false, derr.ErrInvalidArgument.Msg("object_identifier")
}

func (i *ObjectIdentifier) Key() string {
	return i.GetType() + "|" + i.GetKey()
}
