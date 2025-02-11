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
	return &relation{&safe.SafeRelation{
		RelationIdentifier: i,
		HasSubjectRelation: i.SubjectRelation != "",
	}}
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
	buf := newRelationBuffer()

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
	buf := newRelationBuffer()

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

func (i *relation) PathAndFilter(filter *bytes.Buffer) ([]string, error) {
	switch {
	case ObjectSelector(i.Object()).IsComplete():
		i.ObjFilter(filter)
		return bdb.RelationsObjPath, nil
	case ObjectSelector(i.Subject()).IsComplete():
		i.SubFilter(filter)
		return bdb.RelationsSubPath, nil
	default:
		return []string{}, ErrNoCompleteObjectIdentifier
	}
}

// ObjFilter
// format: obj_type : obj_id # relation @ sub_type : sub_id (# sub_relation).
// TODO: if subject relation exists add subject relation to filter clause.
func (i *relation) ObjFilter(buf *bytes.Buffer) {
	buf.WriteString(i.GetObjectType())
	buf.WriteByte(TypeIDSeparator)
	buf.WriteString(i.GetObjectId())
	buf.WriteByte(InstanceSeparator)

	if IsNotSet(i.GetRelation()) {
		return
	}

	buf.WriteString(i.GetRelation())
	buf.WriteByte(InstanceSeparator)

	if IsNotSet(i.GetSubjectType()) {
		return
	}

	buf.WriteString(i.GetSubjectType())
	buf.WriteByte(TypeIDSeparator)

	if IsNotSet(i.GetSubjectId()) {
		return
	}

	buf.WriteString(i.GetSubjectId())
}

// SubFilter
// format: sub_type : sub_id (# sub_relation) | obj_type : obj_id # relation.
// TODO: if subject relation exists add subject relation to filter clause.
func (i *relation) SubFilter(buf *bytes.Buffer) {
	buf.WriteString(i.GetSubjectType())
	buf.WriteByte(TypeIDSeparator)
	buf.WriteString(i.GetSubjectId())
	buf.WriteByte(InstanceSeparator)

	if IsNotSet(i.GetRelation()) {
		return
	}

	buf.WriteString(i.GetRelation())
	buf.WriteByte(InstanceSeparator)

	if IsNotSet(i.GetObjectType()) {
		return
	}

	buf.WriteString(i.GetObjectType())
	buf.WriteByte(TypeIDSeparator)

	if IsNotSet(i.GetObjectId()) {
		return
	}

	buf.WriteString(i.GetObjectId())
}

// nolint: gocritic
func (i *relation) Filter(keyFilter *bytes.Buffer) (path bdb.Path, valueFilter func(*dsc3.RelationIdentifier) bool) {
	// #1  determine if object identifier is complete (has type+id)
	// set index path accordingly
	// set keyFilter to match covering path
	// when no complete object identifier, fallback to a full table scan
	if ObjectIdentifier(i.Object()).IsComplete() {
		path = bdb.RelationsObjPath
		i.ObjFilter(keyFilter)
	} else if ObjectIdentifier(i.Subject()).IsComplete() {
		path = bdb.RelationsSubPath
		i.SubFilter(keyFilter)
	}
	if len(path) == 0 {
		log.Debug().Msg("no covering index path, default to scan of relation object path")
		path = bdb.RelationsObjPath
	}

	// #2 build valueFilter function
	filters := make([]func(item *dsc3.RelationIdentifier) bool, 0, 6)

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

	return path, valueFilter
}

// nolint: gocritic // commentedOutCode
func (i *relation) RelationValueFilter(keyFilter *bytes.Buffer) (path bdb.Path, valueFilter func(*dsc3.Relation) bool) {
	// #1  determine if object identifier is complete (has type+id)
	// set index path accordingly
	// set keyFilter to match covering path
	// when no complete object identifier, fallback to a full table scan
	if ObjectIdentifier(i.Object()).IsComplete() {
		path = bdb.RelationsObjPath
		i.ObjFilter(keyFilter)
	} else if ObjectIdentifier(i.Subject()).IsComplete() {
		path = bdb.RelationsSubPath
		i.SubFilter(keyFilter)
	}
	if len(path) == 0 {
		log.Debug().Msg("no covering index path, default to scan of relation object path")
		path = bdb.RelationsObjPath
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

	return path, valueFilter
}

func newRelationBuffer() *bytes.Buffer {
	return bytes.NewBuffer(make([]byte, 0, maxRelationIdentifierSize))
}
