package ds

// model contains relation related items.

import (
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/aserto-dev/azm"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// Relation.
type relation struct {
	*dsc.Relation
}

func Relation(i *dsc.Relation) *relation { return &relation{i} }

func (i *relation) Key() string {
	return i.ObjKey()
}

func (i *relation) ObjKey() string {
	return i.Object.GetType() + TypeIDSeparator + i.Object.GetKey() +
		InstanceSeparator +
		i.GetRelation() +
		InstanceSeparator +
		i.Subject.GetType() + TypeIDSeparator + i.Subject.GetKey()
}

func (i *relation) SubKey() string {
	return i.Subject.GetType() + TypeIDSeparator + i.Subject.GetKey() +
		InstanceSeparator +
		i.GetRelation() +
		InstanceSeparator +
		i.Object.GetType() + TypeIDSeparator + i.Object.GetKey()
}

func (i *relation) Validate(mc *azm.Model) (bool, error) {

	if i == nil {
		return false, ErrInvalidArgumentRelation.Msg("relation not set (nil)")
	}

	if i.Relation == nil {
		return false, ErrInvalidArgumentRelation.Msg("relation not set (nil)")
	}

	if IsNotSet(i.GetRelation()) {
		return false, ErrInvalidArgumentRelation.Msg("relation")
	}

	if ok, err := ObjectIdentifier(i.Relation.Object).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectIdentifier(i.Relation.Subject).Validate(); !ok {
		return ok, err
	}

	if mc == nil {
		return true, nil
	}

	if !mc.ObjectTypeExists(*i.Relation.Object.Type) {
		return false, derr.ErrObjectTypeNotFound.Msg(*i.Relation.Object.Type)
	}

	if !mc.ObjectTypeExists(*i.Relation.Subject.Type) {
		return false, derr.ErrObjectTypeNotFound.Msg(*i.Relation.Subject.Type)
	}

	if !mc.RelationTypeExists(*i.Relation.Object.Type, i.Relation.Relation) {
		return false, derr.ErrRelationTypeNotFound.Msg(*i.Relation.Object.Type + ":" + i.Relation.Relation)
	}

	return true, nil
}

// RelationIdentifier.
type relationIdentifier struct {
	*dsc.RelationIdentifier
}

func RelationIdentifier(i *dsc.RelationIdentifier) *relationIdentifier { return &relationIdentifier{i} }

func (i *relationIdentifier) Key() string {
	return i.ObjKey()
}

func (i *relationIdentifier) ObjKey() string {
	return i.Object.GetType() + TypeIDSeparator + i.Object.GetKey() +
		InstanceSeparator +
		i.Relation.GetName() +
		InstanceSeparator +
		i.Subject.GetType() + TypeIDSeparator + i.Subject.GetKey()
}

func (i *relationIdentifier) SubKey() string {
	return i.Subject.GetType() + TypeIDSeparator + i.Subject.GetKey() +
		InstanceSeparator +
		i.Relation.GetName() +
		InstanceSeparator +
		i.Object.GetType() + TypeIDSeparator + i.Object.GetKey()
}

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

	if i.RelationIdentifier.Relation != nil && (i.RelationIdentifier.Relation.ObjectType == nil || i.RelationIdentifier.Relation.GetObjectType() == "") {
		i.Relation.ObjectType = i.Object.Type
	}

	if ok, err := RelationTypeIdentifier(i.RelationIdentifier.Relation).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectSelector(i.RelationIdentifier.Subject).Validate(); !ok {
		return ok, err
	}

	return true, nil
}

func (i *relation) Hash() string {
	h := fnv.New64a()
	h.Reset()

	if i != nil && i.Relation != nil {
		if i.Relation.Subject != nil {
			if _, err := h.Write([]byte(i.Relation.Subject.GetKey())); err != nil {
				return DefaultHash
			}
			if _, err := h.Write([]byte(i.Relation.Subject.GetType())); err != nil {
				return DefaultHash
			}
		}
		if _, err := h.Write([]byte(i.Relation.Relation)); err != nil {
			return DefaultHash
		}
		if i.Relation.Object != nil {
			if _, err := h.Write([]byte(i.Relation.Object.GetKey())); err != nil {
				return DefaultHash
			}
			if _, err := h.Write([]byte(i.Relation.Object.GetType())); err != nil {
				return DefaultHash
			}
		}
	}

	return strconv.FormatUint(h.Sum64(), 10)
}

func (i *relationIdentifier) PathAndFilter() ([]string, string, error) {
	switch {
	case ObjectSelector(i.RelationIdentifier.Object).IsComplete():
		return bdb.RelationsObjPath, i.ObjFilter(), nil
	case ObjectSelector(i.RelationIdentifier.Subject).IsComplete():
		return bdb.RelationsSubPath, i.SubFilter(), nil
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

// RelationSelector.
type relationSelector struct {
	*dsc.RelationIdentifier
}

func RelationSelector(i *dsc.RelationIdentifier) *relationSelector { return &relationSelector{i} }

func (i *relationSelector) Validate() (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidRelationIdentifier.Msg("nil")
	}

	if i.RelationIdentifier == nil {
		i.RelationIdentifier = &dsc.RelationIdentifier{
			Subject:  &dsc.ObjectIdentifier{},
			Relation: &dsc.RelationTypeIdentifier{},
			Object:   &dsc.ObjectIdentifier{},
		}
	}

	if i.RelationIdentifier.Subject == nil {
		i.RelationIdentifier.Subject = &dsc.ObjectIdentifier{}
	}

	if i.RelationIdentifier.Relation == nil {
		i.RelationIdentifier.Relation = &dsc.RelationTypeIdentifier{}
	}

	if i.RelationIdentifier.Object == nil {
		i.RelationIdentifier.Object = &dsc.ObjectIdentifier{}
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

	// propagate object type to relation if missing.
	if i.RelationIdentifier.Relation.GetObjectType() == "" {
		i.RelationIdentifier.Relation.ObjectType = i.RelationIdentifier.Object.Type
	}

	// relation:object_type and object:object_type must match
	if i.RelationIdentifier.Relation.GetObjectType() != i.RelationIdentifier.Object.GetType() {
		return false, errors.Wrapf(derr.ErrInvalidObjectType, "conflicting object types relation:%s object:%s",
			i.RelationIdentifier.Relation.GetObjectType(),
			i.RelationIdentifier.Object.GetType(),
		)
	}

	return true, nil
}

type RelationFilter func(*dsc.Relation) bool

func (i *relationSelector) Filter() (bdb.Path, string, RelationFilter) {
	var (
		path      bdb.Path
		keyFilter string
	)

	// #1  determine if object identifier is complete (has type+id)
	// set index path accordingly
	// set keyFilter to match covering path
	// when no complete object identifier, fallback to a full table scan
	if ObjectIdentifier(i.Object).IsComplete() {
		path = bdb.RelationsObjPath
		keyFilter = RelationIdentifier(i.RelationIdentifier).ObjFilter()
	}
	if ObjectIdentifier(i.Subject).IsComplete() {
		path = bdb.RelationsSubPath
		keyFilter = RelationIdentifier(i.RelationIdentifier).SubFilter()
	}
	if len(path) == 0 {
		log.Warn().Msg("!!! no covering index path, full scan !!!")
		path = bdb.RelationsObjPath
		keyFilter = ""
	}

	// #2 build valueFilter function
	filters := []func(item *dsc.Relation) bool{}

	if i.RelationIdentifier.Object.GetType() != "" {
		filters = append(filters, func(item *dsc.Relation) bool {
			return strings.EqualFold(item.Object.GetType(), i.RelationIdentifier.Object.GetType())
		})
	}
	if i.RelationIdentifier.Object.GetKey() != "" {
		filters = append(filters, func(item *dsc.Relation) bool {
			return strings.EqualFold(item.Object.GetKey(), i.RelationIdentifier.Object.GetKey())
		})
	}

	if i.RelationIdentifier.Relation.GetName() != "" {
		filters = append(filters, func(item *dsc.Relation) bool {
			return strings.EqualFold(item.Relation, i.RelationIdentifier.Relation.GetName())
		})
	}

	if i.RelationIdentifier.Subject.GetType() != "" {
		filters = append(filters, func(item *dsc.Relation) bool {
			return strings.EqualFold(item.Subject.GetType(), i.RelationIdentifier.Subject.GetType())
		})
	}
	if i.RelationIdentifier.Subject.GetKey() != "" {
		filters = append(filters, func(item *dsc.Relation) bool {
			return strings.EqualFold(item.Subject.GetKey(), i.RelationIdentifier.Subject.GetKey())
		})
	}

	valueFilter := func(i *dsc.Relation) bool {
		for _, filter := range filters {
			if !filter(i) {
				return false
			}
		}
		return true
	}

	return path, keyFilter, valueFilter
}
