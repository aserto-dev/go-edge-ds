package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type objectTypeIdentifier struct {
	*dsc.ObjectTypeIdentifier
}

func ObjectTypeIdentifier(i *dsc.ObjectTypeIdentifier) *objectTypeIdentifier {
	return &objectTypeIdentifier{i}
}

func (i *objectTypeIdentifier) Validate() (bool, error) {
	if i.ObjectTypeIdentifier == nil {
		return false, derr.ErrInvalidArgument.Msg("object_type_identifier")
	}

	if i.Name != nil && *i.Name != "" {
		return true, nil
	}

	return false, derr.ErrInvalidArgument.Msg("object_type_identifier")
}

func (i *objectTypeIdentifier) Resolve(sc *StoreContext) (*dsc.ObjectTypeIdentifier, error) {
	objType, err := sc.GetObjectType(i.ObjectTypeIdentifier)
	if err != nil {
		return nil, err
	}

	return &dsc.ObjectTypeIdentifier{
		Name: &objType.Name,
	}, nil
}
