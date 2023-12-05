package ds

// model contains relation related items.

import (
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/model"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/rs/zerolog/log"
)

// Relation.
type relation struct {
	*dsc3.Relation
}

func Relation(i *dsc3.Relation) *relation { return &relation{i} }

func GetRelation(i *dsr3.GetRelationRequest) *relation {
	return &relation{&dsc3.Relation{
		ObjectType:      i.ObjectType,
		ObjectId:        i.ObjectId,
		Relation:        i.Relation,
		SubjectType:     i.SubjectType,
		SubjectId:       i.SubjectId,
		SubjectRelation: i.SubjectRelation,
	}}
}

func GetRelations(i *dsr3.GetRelationsRequest) *relation {
	return &relation{&dsc3.Relation{
		ObjectType:      i.ObjectType,
		ObjectId:        i.ObjectId,
		Relation:        i.Relation,
		SubjectType:     i.SubjectType,
		SubjectId:       i.SubjectId,
		SubjectRelation: i.SubjectRelation,
	}}
}

func (i *relation) Key() string {
	return i.ObjKey()
}

func (i *relation) Object() *dsc3.ObjectIdentifier {
	return &dsc3.ObjectIdentifier{
		ObjectType: i.GetObjectType(),
		ObjectId:   i.GetObjectId(),
	}
}

func (i *relation) Subject() *dsc3.ObjectIdentifier {
	return &dsc3.ObjectIdentifier{
		ObjectType: i.GetSubjectType(),
		ObjectId:   i.GetSubjectId(),
	}
}

func (i *relation) ObjKey() string {
	return i.GetObjectType() + TypeIDSeparator + i.GetObjectId() +
		InstanceSeparator +
		i.GetRelation() +
		InstanceSeparator +
		i.GetSubjectType() + TypeIDSeparator + i.GetSubjectId() +
		Iff(i.GetSubjectRelation() == "", "", InstanceSeparator+i.GetSubjectRelation())
}

func (i *relation) SubKey() string {
	return i.GetSubjectType() + TypeIDSeparator + i.GetSubjectId() +
		InstanceSeparator +
		i.GetRelation() +
		InstanceSeparator +
		i.GetObjectType() + TypeIDSeparator + i.GetObjectId() +
		Iff(i.GetSubjectRelation() == "", "", InstanceSeparator+i.GetSubjectRelation())
}

func (i *relation) Validate(mc *cache.Cache) (bool, error) {

	if i == nil {
		return false, ErrInvalidArgumentRelation.Msg("relation not set (nil)")
	}

	if i.Relation == nil {
		return false, ErrInvalidArgumentRelation.Msg("relation not set (nil)")
	}

	if IsNotSet(i.GetRelation()) {
		return false, ErrInvalidArgumentRelation.Msg("relation")
	}

	if ok, err := ObjectIdentifier(i.Object()).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectIdentifier(i.Subject()).Validate(); !ok {
		return ok, err
	}

	if mc == nil {
		return true, nil
	}

	if !mc.ObjectExists(model.ObjectName(i.GetObjectType())) {
		return false, derr.ErrObjectNotFound.Msg(i.GetObjectType())
	}

	if !mc.ObjectExists(model.ObjectName(i.GetSubjectType())) {
		return false, derr.ErrObjectNotFound.Msg(i.GetSubjectType())
	}

	if !mc.RelationExists(model.ObjectName(i.GetObjectType()), model.RelationName(i.GetRelation())) {
		return false, derr.ErrRelationNotFound.Msg(i.GetObjectType() + ":" + i.GetRelation())
	}

	return true, nil
}

func (i *relation) Hash() string {
	h := fnv.New64a()
	h.Reset()

	if i != nil && i.Relation != nil {
		if _, err := h.Write([]byte(i.GetObjectId())); err != nil {
			return DefaultHash
		}
		if _, err := h.Write([]byte(i.GetObjectType())); err != nil {
			return DefaultHash
		}
		if _, err := h.Write([]byte(i.GetRelation())); err != nil {
			return DefaultHash
		}
		if _, err := h.Write([]byte(i.GetSubjectId())); err != nil {
			return DefaultHash
		}
		if _, err := h.Write([]byte(i.GetSubjectType())); err != nil {
			return DefaultHash
		}
		if _, err := h.Write([]byte(i.GetSubjectRelation())); err != nil {
			return DefaultHash
		}
	}

	return strconv.FormatUint(h.Sum64(), 10)
}

func (i *relation) PathAndFilter() ([]string, string, error) {
	switch {
	case ObjectSelector(i.Object()).IsComplete():
		return bdb.RelationsObjPath, i.ObjFilter(), nil
	case ObjectSelector(i.Subject()).IsComplete():
		return bdb.RelationsSubPath, i.SubFilter(), nil
	default:
		return []string{}, "", ErrNoCompleteObjectIdentifier
	}
}

// ObjFilter
// format: obj_type : obj_id # relation @ sub_type : sub_id (# sub_relation).
// TODO: if subject relation exists add subject relation to filter clause.
func (i *relation) ObjFilter() string {
	filter := strings.Builder{}

	filter.WriteString(i.GetObjectType())
	filter.WriteString(TypeIDSeparator)
	filter.WriteString(i.GetObjectId())
	filter.WriteString(InstanceSeparator)

	if IsNotSet(i.GetRelation()) {
		return filter.String()
	}

	filter.WriteString(i.GetRelation())
	filter.WriteString(InstanceSeparator)

	if IsNotSet(i.GetSubjectType()) {
		return filter.String()
	}

	filter.WriteString(i.GetSubjectType())
	filter.WriteString(TypeIDSeparator)

	if IsNotSet(i.GetSubjectId()) {
		return filter.String()
	}

	filter.WriteString(i.GetSubjectId())

	return filter.String()
}

// SubFilter
// format: sub_type : sub_id (# sub_relation) | obj_type : obj_id # relation.
// TODO: if subject relation exists add subject relation to filter clause.
func (i *relation) SubFilter() string {
	filter := strings.Builder{}

	filter.WriteString(i.GetSubjectType())
	filter.WriteString(TypeIDSeparator)
	filter.WriteString(i.GetSubjectId())
	filter.WriteString(InstanceSeparator)

	if IsNotSet(i.GetRelation()) {
		return filter.String()
	}

	filter.WriteString(i.GetRelation())
	filter.WriteString(InstanceSeparator)

	if IsNotSet(i.GetObjectType()) {
		return filter.String()
	}

	filter.WriteString(i.GetObjectType())
	filter.WriteString(TypeIDSeparator)

	if IsNotSet(i.GetObjectId()) {
		return filter.String()
	}

	filter.WriteString(i.GetObjectId())

	return filter.String()
}

type RelationFilter func(*dsc3.Relation) bool

func (i *relation) Filter() (bdb.Path, string, RelationFilter) {
	var (
		path      bdb.Path
		keyFilter string
	)

	// #1  determine if object identifier is complete (has type+id)
	// set index path accordingly
	// set keyFilter to match covering path
	// when no complete object identifier, fallback to a full table scan
	if ObjectIdentifier(i.Object()).IsComplete() {
		path = bdb.RelationsObjPath
		keyFilter = i.ObjFilter()
	}
	if ObjectIdentifier(i.Subject()).IsComplete() {
		path = bdb.RelationsSubPath
		keyFilter = i.SubFilter()
	}
	if len(path) == 0 {
		log.Debug().Msg("no covering index path, default to scan of relation object path")
		path = bdb.RelationsObjPath
		keyFilter = ""
	}

	// #2 build valueFilter function
	filters := []func(item *dsc3.Relation) bool{}

	if fv := i.GetObjectType(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(item.GetObjectType(), fv)
			log.Trace().Str("fv", fv).Str("item", item.GetObjectType()).Bool("equal", equal == 0).Msg("object_type filter")
			return equal == 0
		})
	}

	if fv := i.GetObjectId(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(fv, item.GetObjectId())
			log.Trace().Str("fv", fv).Str("item", item.GetObjectId()).Bool("equal", equal == 0).Msg("object_id filter")
			return equal == 0
		})
	}

	if fv := i.GetRelation(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(item.Relation, fv)
			log.Trace().Str("fv", fv).Str("item", item.Relation).Bool("equal", equal == 0).Msg("relation filter")
			return equal == 0
		})
	}

	if fv := i.GetSubjectType(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(item.GetSubjectType(), fv)
			log.Trace().Str("fv", fv).Str("item", item.GetSubjectType()).Bool("equal", equal == 0).Msg("subject_type filter")
			return equal == 0
		})
	}

	if fv := i.GetSubjectId(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(fv, item.GetSubjectId())
			log.Trace().Str("fv", fv).Str("item", item.GetSubjectId()).Bool("equal", equal == 0).Msg("subject_id filter")
			return equal == 0
		})
	}

	if fv := i.GetSubjectRelation(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(item.SubjectRelation, fv)
			log.Trace().Str("fv", fv).Str("item", item.SubjectRelation).Bool("equal", equal == 0).Msg("subject_relation filter")
			return equal == 0
		})
	}

	valueFilter := func(i *dsc3.Relation) bool {
		for _, filter := range filters {
			if !filter(i) {
				return false
			}
		}
		return true
	}

	return path, keyFilter, valueFilter
}
