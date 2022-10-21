package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type ObjectIdentifier struct {
	*dsc.ObjectIdentifier
}

func NewObjectIdentifier(i *dsc.ObjectIdentifier) *ObjectIdentifier {
	if i == nil {
		return &ObjectIdentifier{ObjectIdentifier: &dsc.ObjectIdentifier{}}
	}
	return &ObjectIdentifier{ObjectIdentifier: i}
}

func (i *ObjectIdentifier) Msg() *dsc.ObjectIdentifier {
	return i.ObjectIdentifier
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

func (i *ObjectIdentifier) Resolve(sc *StoreContext) (*ObjectIdentifier, error) {
	obj, err := sc.GetObject(i)
	if err != nil {
		return nil, err
	}

	return &ObjectIdentifier{
		&dsc.ObjectIdentifier{
			Id:   &obj.Id,
			Type: &obj.Type,
			Key:  &obj.Key,
		},
	}, nil
}
