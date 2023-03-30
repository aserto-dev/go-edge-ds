package ds

// model contains relation related items.

import (
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
)

// RelationIdentifier.
type relationIdentifier struct {
	*dsc.RelationIdentifier
}

func RelationIdentifier(i *dsc.RelationIdentifier) *relationIdentifier { return &relationIdentifier{i} }

func (i *relationIdentifier) Validate() (bool, error) {

	if i == nil {
		return false, derr.ErrInvalidRelationIdentifier.Msg("nil")
	}

	if i.RelationIdentifier == nil {
		return false, derr.ErrInvalidArgument.Msg("relation_identifier")
	}

	if ok, err := ObjectSelector(i.RelationIdentifier.Object).Validate(); !ok {
		return ok, err
	}

	if ok, err := RelationTypeSelector(i.RelationIdentifier.Relation).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectSelector(i.RelationIdentifier.Subject).Validate(); !ok {
		return ok, err
	}

	return true, nil
}

func (i *relationIdentifier) PathAndFilter() ([]string, string, error) {
	switch {
	case ObjectSelector(i.RelationIdentifier.Object).IsComplete():
		return RelationsObjPath, i.ObjFilter(), nil
	case ObjectSelector(i.RelationIdentifier.Subject).IsComplete():
		return RelationsSubPath, i.SubFilter(), nil
	default:
		return []string{}, "", ErrNoCompleteObjectIdentifier
	}
}

// ObjFilter
// format: obj_type : obj_id # relation @ sub_type : sub_id (# sub_relation).
// TODO: if subject relation exists add subject relation to filter clause.
func (i *relationIdentifier) ObjFilter() string {
	filter := strings.Builder{}

	filter.WriteString(i.GetObject().GetType())
	filter.WriteByte(':')
	filter.WriteString(i.GetObject().GetKey())
	filter.WriteByte('|')

	if IsNotSet(i.GetRelation().GetName()) {
		return filter.String()
	}

	filter.WriteString(i.GetRelation().GetName())
	filter.WriteByte('|')

	if IsNotSet(i.GetSubject().GetType()) {
		return filter.String()
	}

	filter.WriteString(i.GetSubject().GetType())
	filter.WriteByte(':')

	if IsNotSet(i.GetSubject().GetKey()) {
		return filter.String()
	}

	filter.WriteString(i.GetSubject().GetKey())

	return filter.String()
}

// SubFilter
// format: sub_type : sub_id (# sub_relation) | obj_type : obj_id # relation.
// TODO: if subject relation exists add subject relation to filter clause.
func (i *relationIdentifier) SubFilter() string {
	filter := strings.Builder{}

	filter.WriteString(i.GetSubject().GetType())
	filter.WriteByte(':')
	filter.WriteString(i.GetSubject().GetKey())
	filter.WriteByte('|')

	if IsNotSet(i.GetRelation().GetName()) {
		return filter.String()
	}

	filter.WriteString(i.GetRelation().GetName())
	filter.WriteByte('|')

	if IsNotSet(i.GetObject().GetType()) {
		return filter.String()
	}

	filter.WriteString(i.GetObject().GetType())
	filter.WriteByte(':')

	if IsNotSet(i.GetObject().GetKey()) {
		return filter.String()
	}

	filter.WriteString(i.GetObject().GetKey())

	return filter.String()
}

// RelationTypeIdentifier.
type relationTypeIdentifier struct {
	*dsc.RelationTypeIdentifier
}

func RelationTypeIdentifier(i *dsc.RelationTypeIdentifier) *relationTypeIdentifier {
	return &relationTypeIdentifier{i}
}

func (i *relationTypeIdentifier) Validate() (bool, error) {
	// TODO : validate that object type exists in type system.
	if i.RelationTypeIdentifier == nil {
		return false, ErrInvalidArgumentRelationTypeIdentifier.Msg("not set (nil)")
	}

	if IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentRelationTypeIdentifier.Msg("name")
	}

	return true, nil
}

// RelationTypeSelector.
type relationTypeSelector struct {
	*dsc.RelationTypeIdentifier
}

func RelationTypeSelector(i *dsc.RelationTypeIdentifier) *relationTypeSelector {
	return &relationTypeSelector{i}
}

func (i *relationTypeSelector) Validate() (bool, error) {
	if i.RelationTypeIdentifier == nil {
		return false, ErrInvalidArgumentRelationTypeIdentifier.Msg("not set(nil)")
	}

	// TODO : validate that if Name is set, the object type exists in type system.

	return true, nil
}
