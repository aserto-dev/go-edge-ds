package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type ObjectTypeIdentifier struct {
	*dsc.ObjectTypeIdentifier
}

func NewObjectTypeIdentifier(i *dsc.ObjectTypeIdentifier) *ObjectTypeIdentifier {
	if i == nil {
		return &ObjectTypeIdentifier{ObjectTypeIdentifier: &dsc.ObjectTypeIdentifier{}}
	}
	return &ObjectTypeIdentifier{ObjectTypeIdentifier: i}
}

func (i *ObjectTypeIdentifier) Msg() *dsc.ObjectTypeIdentifier {
	return i.ObjectTypeIdentifier
}

func (i *ObjectTypeIdentifier) Validate() (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidArgument.Msg("object_type_identifier")
	}

	if i.Id != nil && *i.Id > 0 {
		return true, nil
	}
	if i.Name != nil && *i.Name != "" {
		return true, nil
	}

	return false, derr.ErrInvalidArgument.Msg("object_type_identifier")
}

func (i *ObjectTypeIdentifier) Resolve(sc *StoreContext) (*ObjectTypeIdentifier, error) {
	objType, err := sc.GetObjectType(i)
	if err != nil {
		return nil, err
	}

	return &ObjectTypeIdentifier{
		&dsc.ObjectTypeIdentifier{
			Id:   &objType.Id,
			Name: &objType.Name,
		},
	}, nil
}
