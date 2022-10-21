package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type RelationIdentifier struct {
	*dsc.RelationIdentifier
}

func NewRelationIdentifier(i *dsc.RelationIdentifier) *RelationIdentifier {
	if i == nil {
		return &RelationIdentifier{RelationIdentifier: &dsc.RelationIdentifier{
			Subject:  &dsc.ObjectIdentifier{},
			Relation: &dsc.RelationTypeIdentifier{},
			Object:   &dsc.ObjectIdentifier{},
		}}
	}
	return &RelationIdentifier{RelationIdentifier: i}
}

func (i *RelationIdentifier) Msg() *dsc.RelationIdentifier {
	return i.RelationIdentifier
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

func (i *RelationIdentifier) Resolve(sc *StoreContext) (*RelationIdentifier, error) {
	s, err := NewObjectIdentifier(i.Subject).Resolve(sc)
	if err != nil {
		return nil, err
	}

	r, err := NewRelationTypeIdentifier(i.Relation).Resolve(sc)
	if err != nil {
		return nil, err
	}

	o, err := NewObjectIdentifier(i.Object).Resolve(sc)
	if err != nil {
		return nil, err
	}

	return &RelationIdentifier{
		&dsc.RelationIdentifier{
			Subject:  s.Msg(),
			Relation: r.Msg(),
			Object:   o.Msg(),
		},
	}, nil
}
