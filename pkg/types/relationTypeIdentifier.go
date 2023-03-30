package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type relationTypeIdentifier struct {
	*dsc.RelationTypeIdentifier
}

func RelationTypeIdentifier(i *dsc.RelationTypeIdentifier) *relationTypeIdentifier {
	return &relationTypeIdentifier{i}
}

func (i *relationTypeIdentifier) Validate() (bool, error) {
	if i.RelationTypeIdentifier == nil {
		return false, derr.ErrInvalidArgument.Msg("relation_type_identifier")
	}

	if i.Name != nil && i.ObjectType != nil &&
		i.GetName() != "" && i.GetObjectType() != "" {
		return true, nil
	}

	return false, derr.ErrInvalidArgument.Msg("relation_type_identifier")
}

func (i *relationTypeIdentifier) Key() string {
	return i.GetObjectType() + ":" + i.GetName()
}

func (i *relationTypeIdentifier) Resolve(sc *StoreContext) (*dsc.RelationTypeIdentifier, error) {
	relType, err := sc.GetRelationType(i.RelationTypeIdentifier)
	if err != nil {
		return nil, err
	}

	return &dsc.RelationTypeIdentifier{
		Name:       &relType.Name,
		ObjectType: &relType.ObjectType,
	}, nil
}
