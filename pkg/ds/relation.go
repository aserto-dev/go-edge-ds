package ds

// model contains relation related items.

import (
	"hash/fnv"
	"strconv"
	"strings"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// Relation.
type relation struct {
	*dsc2.Relation
}

func Relation(i *dsc2.Relation) *relation { return &relation{i} }

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

func (i *relation) Validate() (bool, error) {

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

	return true, nil
}

// RelationIdentifier.
type relationIdentifier struct {
	*dsc2.RelationIdentifier
}

func RelationIdentifier(i *dsc2.RelationIdentifier) *relationIdentifier {
	return &relationIdentifier{i}
}

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
	*dsc2.RelationIdentifier
}

func RelationSelector(i *dsc2.RelationIdentifier) *relationSelector { return &relationSelector{i} }

func (i *relationSelector) Validate() (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidRelationIdentifier.Msg("nil")
	}

	if i.RelationIdentifier == nil {
		i.RelationIdentifier = &dsc2.RelationIdentifier{
			Subject:  &dsc2.ObjectIdentifier{},
			Relation: &dsc2.RelationTypeIdentifier{},
			Object:   &dsc2.ObjectIdentifier{},
		}
	}

	if i.RelationIdentifier.Subject == nil {
		i.RelationIdentifier.Subject = &dsc2.ObjectIdentifier{}
	}

	if i.RelationIdentifier.Relation == nil {
		i.RelationIdentifier.Relation = &dsc2.RelationTypeIdentifier{}
	}

	if i.RelationIdentifier.Object == nil {
		i.RelationIdentifier.Object = &dsc2.ObjectIdentifier{}
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

	// if relation name is set, propagate object type to relation if missing.
	if i.RelationIdentifier.Relation.GetName() != "" && i.RelationIdentifier.Relation.GetObjectType() == "" {
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

type RelationFilter func(*dsc2.Relation) bool

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
	filters := []func(item *dsc2.Relation) bool{}

	if i.RelationIdentifier.Object.GetType() != "" {
		fv := i.RelationIdentifier.Object.GetType()
		filters = append(filters, func(item *dsc2.Relation) bool {
			equal := strings.EqualFold(item.Object.GetType(), fv)
			log.Trace().Str("fv", fv).Str("item", item.Object.GetType()).Bool("equal", equal).Msg("object_type filter")
			return equal
		})
	}
	if i.RelationIdentifier.Object.GetKey() != "" {
		fv := i.RelationIdentifier.Object.GetKey()
		filters = append(filters, func(item *dsc2.Relation) bool {
			equal := strings.Compare(fv, item.Object.GetKey())
			log.Trace().Str("fv", fv).Str("item", item.Object.GetKey()).Bool("equal", equal == 0).Msg("object_id filter")
			return equal == 0
		})
	}

	if i.RelationIdentifier.Relation.GetName() != "" {
		fv := i.RelationIdentifier.Relation.GetName()
		filters = append(filters, func(item *dsc2.Relation) bool {
			equal := strings.EqualFold(item.Relation, fv)
			log.Trace().Str("fv", fv).Str("item", item.Relation).Bool("equal", equal).Msg("relation filter")
			return equal
		})
	}

	if i.RelationIdentifier.Subject.GetType() != "" {
		fv := i.RelationIdentifier.Subject.GetType()
		filters = append(filters, func(item *dsc2.Relation) bool {
			equal := strings.EqualFold(item.Subject.GetType(), fv)
			log.Trace().Str("fv", fv).Str("item", item.Subject.GetType()).Bool("equal", equal).Msg("subject_type filter")
			return equal
		})
	}
	if i.RelationIdentifier.Subject.GetKey() != "" {
		fv := i.RelationIdentifier.Subject.GetKey()
		filters = append(filters, func(item *dsc2.Relation) bool {
			equal := strings.Compare(fv, item.Subject.GetKey())
			log.Trace().Str("fv", fv).Str("item", item.Subject.GetKey()).Bool("equal", equal == 0).Msg("subject_id filter")
			return equal == 0
		})
	}

	valueFilter := func(i *dsc2.Relation) bool {
		for _, filter := range filters {
			if !filter(i) {
				return false
			}
		}
		return true
	}

	return path, keyFilter, valueFilter
}
