package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type RelationIdentifier struct {
	*dsc.RelationIdentifier
}

func (i *RelationIdentifier) Validate() (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidArgument.Msg("relation_identifier")
	}

	subject := ObjectIdentifier{ObjectIdentifier: i.Subject}
	if ok, err := subject.Validate(); !ok {
		return false, err
	}

	relation := RelationTypeIdentifier{RelationTypeIdentifier: i.Relation}
	if ok, err := relation.Validate(); !ok {
		return false, err
	}

	object := ObjectIdentifier{ObjectIdentifier: i.Object}
	if ok, err := object.Validate(); !ok {
		return false, err
	}

	return true, nil
}

func (i *RelationIdentifier) ObjKey() string {
	if i.Relation.GetObjectType() == "" && i.Object.GetType() != "" {
		i.Relation.ObjectType = i.Object.Type
	}
	return i.Object.GetId() + "|" + i.Object.GetType() + ":" + i.Relation.GetName() + "|" + i.Subject.GetId()
}

func (i *RelationIdentifier) SubKey() string {
	if i.Relation.GetObjectType() == "" && i.Object.GetType() != "" {
		i.Relation.ObjectType = i.Object.Type
	}
	return i.Subject.GetId() + "|" + i.Object.GetType() + ":" + i.Relation.GetName() + "|" + i.Object.GetId()
}
