package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type RelationTypeIdentifier struct {
	*dsc.RelationTypeIdentifier
}

func NewRelationTypeIdentifier(i *dsc.RelationTypeIdentifier) *RelationTypeIdentifier {
	if i == nil {
		return &RelationTypeIdentifier{RelationTypeIdentifier: &dsc.RelationTypeIdentifier{}}
	}
	return &RelationTypeIdentifier{RelationTypeIdentifier: i}
}

func (i *RelationTypeIdentifier) Msg() *dsc.RelationTypeIdentifier {
	return i.RelationTypeIdentifier
}

func (i *RelationTypeIdentifier) Validate() (bool, error) {
	if i.RelationTypeIdentifier == nil {
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

func (i *RelationTypeIdentifier) Key() string {
	return i.GetObjectType() + ":" + i.GetName()
}

func (i *RelationTypeIdentifier) Resolve(sc *StoreContext) (*RelationTypeIdentifier, error) {
	relType, err := sc.GetRelationType(i)
	if err != nil {
		return nil, err
	}

	return &RelationTypeIdentifier{
		&dsc.RelationTypeIdentifier{
			Id:         &relType.Id,
			Name:       &relType.Name,
			ObjectType: &relType.ObjectType,
		},
	}, nil
}
