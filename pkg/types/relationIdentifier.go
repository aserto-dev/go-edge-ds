package types

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

type relationIdentifier struct {
	*dsc.RelationIdentifier
}

func RelationIdentifier(i *dsc.RelationIdentifier) *relationIdentifier { return &relationIdentifier{i} }

func (i *relationIdentifier) Validate() (bool, error) {
	if i.RelationIdentifier == nil {
		return false, derr.ErrInvalidArgument.Msg("relation_identifier")
	}

	if ok, err := ObjectIdentifier(i.Subject).Validate(); !ok {
		return false, err
	}

	if ok, err := RelationTypeIdentifier(i.Relation).Validate(); !ok {
		return false, err
	}

	if ok, err := ObjectIdentifier(i.Object).Validate(); !ok {
		return false, err
	}

	return true, nil
}

func (i *relationIdentifier) ObjKey() string {
	if i.Relation.GetObjectType() == "" && i.Object.GetType() != "" {
		i.Relation.ObjectType = i.Object.Type
	}
	return i.Object.GetKey() + "|" + i.Object.GetType() + ":" + i.Relation.GetName() + "|" + i.Subject.GetKey()
}

func (i *relationIdentifier) SubKey() string {
	if i.Relation.GetObjectType() == "" && i.Object.GetType() != "" {
		i.Relation.ObjectType = i.Object.Type
	}
	return i.Subject.GetKey() + "|" + i.Object.GetType() + ":" + i.Relation.GetName() + "|" + i.Object.GetKey()
}

func (i *relationIdentifier) Resolve(sc *StoreContext) (*dsc.RelationIdentifier, error) {
	s, err := ObjectIdentifier(i.Subject).Resolve(sc)
	if err != nil {
		return nil, err
	}

	r, err := RelationTypeIdentifier(i.Relation).Resolve(sc)
	if err != nil {
		return nil, err
	}

	o, err := ObjectIdentifier(i.Object).Resolve(sc)
	if err != nil {
		return nil, err
	}

	return &dsc.RelationIdentifier{
		Subject:  s,
		Relation: r,
		Object:   o,
	}, nil
}
