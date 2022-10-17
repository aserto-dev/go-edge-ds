package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type relationTypeIdentifier struct{}

var RelationTypeIdentifier = relationTypeIdentifier{}

func (relationTypeIdentifier) Validate(i *dsc.RelationTypeIdentifier) (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidArgument.Msg("relation_type_identifier")
	}

	if i.Id != nil && *i.Id > 0 {
		return true, nil
	}
	if i.Name != nil && i.ObjectType != nil &&
		*i.Name != "" && *i.ObjectType != "" {
		return true, nil
	}

	return false, derr.ErrInvalidArgument.Msg("relation_type_identifier")
}
