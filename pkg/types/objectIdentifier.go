package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type objectIdentifier struct {
	*dsc.ObjectIdentifier
}

func ObjectIdentifier(i *dsc.ObjectIdentifier) *objectIdentifier { return &objectIdentifier{i} }

func (i *objectIdentifier) Validate() (bool, error) {
	if i.ObjectIdentifier == nil {
		return false, derr.ErrInvalidArgument.Msg("object_identifier")
	}
	// TODO: validate type existence against TypeSystem model
	if i.GetKey() != "" && i.GetType() != "" {
		return true, nil
	}
	// TODO: validate type only usage should be covered by ObjectSelector
	if i.GetType() != "" {
		return true, nil
	}
	return false, derr.ErrInvalidArgument.Msg("object_identifier")
}

func (i *objectIdentifier) Key() string {
	return i.GetType() + ":" + i.GetKey()
}

func (i *objectIdentifier) Resolve(sc *StoreContext) (*dsc.ObjectIdentifier, error) {
	obj, err := sc.GetObject(i.ObjectIdentifier)
	if err != nil {
		return nil, err
	}

	return &dsc.ObjectIdentifier{
		Type: &obj.Type,
		Key:  &obj.Key,
	}, nil
}
