package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type relationIdentifier struct{}

var RelationIdentifier = relationIdentifier{}

func (relationIdentifier) Validate(i *dsc.RelationIdentifier) (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidArgument.Msg("relation_identifier")
	}
	if ok, err := ObjectIdentifier.Validate(i.Subject); !ok {
		return false, err
	}
	if ok, err := RelationTypeIdentifier.Validate(i.Relation); !ok {
		return false, err
	}
	if ok, err := ObjectIdentifier.Validate(i.Object); !ok {
		return false, err
	}
	return true, nil
}
