package ds

// model contains relation related items.

import (
	"bytes"
	"strings"

	"github.com/aserto-dev/azm/safe"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/rs/zerolog/log"
)

// Relation identifier.
type relation struct {
	*safe.SafeRelation
}

// Relation selector.
type relations struct {
	*safe.SafeRelations // implements Validate
	relation            // implements Filter
}

func Relation(i *dsc3.Relation) *relation {
	return &relation{safe.Relation(&dsc3.RelationIdentifier{
		ObjectType:      i.ObjectType,
		ObjectId:        i.ObjectId,
		Relation:        i.Relation,
		SubjectType:     i.SubjectType,
		SubjectId:       i.SubjectId,
		SubjectRelation: i.SubjectRelation,
	})}
}

func RelationIdentifier(i *dsc3.RelationIdentifier) *relation {
	return &relation{safe.Relation(i)}
}

func GetRelation(i *dsr3.GetRelationRequest) *relations {
	r := safe.GetRelation(i)
	return &relations{r, relation{r.SafeRelation}}
}

func GetRelations(i *dsr3.GetRelationsRequest) *relations {
	r := safe.GetRelations(i)
	return &relations{r, relation{r.SafeRelation}}
}

func (i *relation) Key() []byte {
	return i.ObjKey()
}

func (i *relation) ObjKey() []byte {
	var buf bytes.Buffer
	buf.Grow(832)

	buf.WriteString(i.GetObjectType())
	buf.WriteByte(TypeIDSeparator)
	buf.WriteString(i.GetObjectId())

	buf.WriteByte(InstanceSeparator)
	buf.WriteString(i.GetRelation())
	buf.WriteByte(InstanceSeparator)

	buf.WriteString(i.GetSubjectType())
	buf.WriteByte(TypeIDSeparator)
	buf.WriteString(i.GetSubjectId())

	if i.GetSubjectRelation() != "" {
		buf.WriteByte(InstanceSeparator)
		buf.WriteString(i.GetSubjectRelation())
	}

	return buf.Bytes()
}

func (i *relation) SubKey() []byte {
	var buf bytes.Buffer
	buf.Grow(832)

	buf.WriteString(i.GetSubjectType())
	buf.WriteByte(TypeIDSeparator)
	buf.WriteString(i.GetSubjectId())

	buf.WriteByte(InstanceSeparator)
	buf.WriteString(i.GetRelation())
	buf.WriteByte(InstanceSeparator)

	buf.WriteString(i.GetObjectType())
	buf.WriteByte(TypeIDSeparator)
	buf.WriteString(i.GetObjectId())

	if i.GetSubjectRelation() != "" {
		buf.WriteByte(InstanceSeparator)
		buf.WriteString(i.GetSubjectRelation())
	}

	return buf.Bytes()
}

func (i *relation) PathAndFilter() ([]string, []byte, error) {
	switch {
	case ObjectSelector(i.Object()).IsComplete():
		return bdb.RelationsObjPath, i.ObjFilter(), nil
	case ObjectSelector(i.Subject()).IsComplete():
		return bdb.RelationsSubPath, i.SubFilter(), nil
	default:
		return []string{}, []byte{}, ErrNoCompleteObjectIdentifier
	}
}

// ObjFilter
// format: obj_type : obj_id # relation @ sub_type : sub_id (# sub_relation).
// TODO: if subject relation exists add subject relation to filter clause.
func (i *relation) ObjFilter() []byte {
	var buf bytes.Buffer
	buf.Grow(832)

	buf.WriteString(i.GetObjectType())
	buf.WriteByte(TypeIDSeparator)
	buf.WriteString(i.GetObjectId())
	buf.WriteByte(InstanceSeparator)

	if IsNotSet(i.GetRelation()) {
		return buf.Bytes()
	}

	buf.WriteString(i.GetRelation())
	buf.WriteByte(InstanceSeparator)

	if IsNotSet(i.GetSubjectType()) {
		return buf.Bytes()
	}

	buf.WriteString(i.GetSubjectType())
	buf.WriteByte(TypeIDSeparator)

	if IsNotSet(i.GetSubjectId()) {
		return buf.Bytes()
	}

	buf.WriteString(i.GetSubjectId())

	return buf.Bytes()
}

// SubFilter
// format: sub_type : sub_id (# sub_relation) | obj_type : obj_id # relation.
// TODO: if subject relation exists add subject relation to filter clause.
func (i *relation) SubFilter() []byte {
	var buf bytes.Buffer
	buf.Grow(832)

	buf.WriteString(i.GetSubjectType())
	buf.WriteByte(TypeIDSeparator)
	buf.WriteString(i.GetSubjectId())
	buf.WriteByte(InstanceSeparator)

	if IsNotSet(i.GetRelation()) {
		return buf.Bytes()
	}

	buf.WriteString(i.GetRelation())
	buf.WriteByte(InstanceSeparator)

	if IsNotSet(i.GetObjectType()) {
		return buf.Bytes()
	}

	buf.WriteString(i.GetObjectType())
	buf.WriteByte(TypeIDSeparator)

	if IsNotSet(i.GetObjectId()) {
		return buf.Bytes()
	}

	buf.WriteString(i.GetObjectId())

	return buf.Bytes()
}

// nolint: gocritic
func (i *relation) Filter() (path bdb.Path, keyFilter []byte, valueFilter func(*dsc3.RelationIdentifier) bool) {
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
		keyFilter = []byte{}
	}

	// #2 build valueFilter function
	filters := []func(item *dsc3.RelationIdentifier) bool{}

	if fv := i.GetObjectType(); fv != "" {
		filters = append(filters, func(item *dsc3.RelationIdentifier) bool {
			equal := strings.Compare(item.GetObjectType(), fv)
			// log.Trace().Str("fv", fv).Str("item", item.GetObjectType()).Bool("equal", equal == 0).Msg("object_type filter")
			return equal == 0
		})
	}

	if fv := i.GetObjectId(); fv != "" {
		filters = append(filters, func(item *dsc3.RelationIdentifier) bool {
			equal := strings.Compare(fv, item.GetObjectId())
			// log.Trace().Str("fv", fv).Str("item", item.GetObjectId()).Bool("equal", equal == 0).Msg("object_id filter")
			return equal == 0
		})
	}

	if fv := i.GetRelation(); fv != "" {
		filters = append(filters, func(item *dsc3.RelationIdentifier) bool {
			equal := strings.Compare(item.Relation, fv)
			// log.Trace().Str("fv", fv).Str("item", item.Relation).Bool("equal", equal == 0).Msg("relation filter")
			return equal == 0
		})
	}

	if fv := i.GetSubjectType(); fv != "" {
		filters = append(filters, func(item *dsc3.RelationIdentifier) bool {
			equal := strings.Compare(item.GetSubjectType(), fv)
			// log.Trace().Str("fv", fv).Str("item", item.GetSubjectType()).Bool("equal", equal == 0).Msg("subject_type filter")
			return equal == 0
		})
	}

	if fv := i.GetSubjectId(); fv != "" {
		filters = append(filters, func(item *dsc3.RelationIdentifier) bool {
			equal := strings.Compare(fv, item.GetSubjectId())
			// log.Trace().Str("fv", fv).Str("item", item.GetSubjectId()).Bool("equal", equal == 0).Msg("subject_id filter")
			return equal == 0
		})
	}

	if i.HasSubjectRelation {
		fv := i.GetSubjectRelation()
		filters = append(filters, func(item *dsc3.RelationIdentifier) bool {
			equal := strings.Compare(item.SubjectRelation, fv)
			// log.Trace().Str("fv", fv).Str("item", item.SubjectRelation).Bool("equal", equal == 0).Msg("subject_relation filter")
			return equal == 0
		})
	}

	valueFilter = func(i *dsc3.RelationIdentifier) bool {
		for _, filter := range filters {
			if !filter(i) {
				return false
			}
		}
		return true
	}

	return path, keyFilter, valueFilter
}

func (i *relation) RelationValueFilter() (path bdb.Path, keyFilter []byte, valueFilter func(*dsc3.Relation) bool) {
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
		keyFilter = []byte{}
	}

	// #2 build valueFilter function
	filters := []func(item *dsc3.Relation) bool{}

	if fv := i.GetObjectType(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(item.GetObjectType(), fv)
			// log.Trace().Str("fv", fv).Str("item", item.GetObjectType()).Bool("equal", equal == 0).Msg("object_type filter")
			return equal == 0
		})
	}

	if fv := i.GetObjectId(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(fv, item.GetObjectId())
			// log.Trace().Str("fv", fv).Str("item", item.GetObjectId()).Bool("equal", equal == 0).Msg("object_id filter")
			return equal == 0
		})
	}

	if fv := i.GetRelation(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(item.Relation, fv)
			// log.Trace().Str("fv", fv).Str("item", item.Relation).Bool("equal", equal == 0).Msg("relation filter")
			return equal == 0
		})
	}

	if fv := i.GetSubjectType(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(item.GetSubjectType(), fv)
			// log.Trace().Str("fv", fv).Str("item", item.GetSubjectType()).Bool("equal", equal == 0).Msg("subject_type filter")
			return equal == 0
		})
	}

	if fv := i.GetSubjectId(); fv != "" {
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(fv, item.GetSubjectId())
			// log.Trace().Str("fv", fv).Str("item", item.GetSubjectId()).Bool("equal", equal == 0).Msg("subject_id filter")
			return equal == 0
		})
	}

	if i.HasSubjectRelation {
		fv := i.GetSubjectRelation()
		filters = append(filters, func(item *dsc3.Relation) bool {
			equal := strings.Compare(item.SubjectRelation, fv)
			// log.Trace().Str("fv", fv).Str("item", item.SubjectRelation).Bool("equal", equal == 0).Msg("subject_relation filter")
			return equal == 0
		})
	}

	valueFilter = func(i *dsc3.Relation) bool {
		for _, filter := range filters {
			if !filter(i) {
				return false
			}
		}
		return true
	}

	return path, keyFilter, valueFilter
}
