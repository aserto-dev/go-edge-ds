package ds

// model contains relation related items.

import (
	"strings"

	"github.com/aserto-dev/azm/safe"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
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

func Relation(i *dsc3.Relation) *relation { return &relation{safe.Relation(i)} }

func GetRelation(i *dsr3.GetRelationRequest) *relations {
	r := safe.GetRelation(i)
	return &relations{r, relation{r.SafeRelation}}
}

func GetRelations(i *dsr3.GetRelationsRequest) *relations {
	r := safe.GetRelations(i)
	return &relations{r, relation{r.SafeRelation}}
}

func (i *relation) Key() string {
	return i.ObjKey()
}

func (i *relation) ObjKey() string {
	return i.GetObjectType() + TypeIDSeparator + i.GetObjectId() +
		InstanceSeparator +
		i.GetRelation() +
		InstanceSeparator +
		i.GetSubjectType() + TypeIDSeparator + i.GetSubjectId() +
		lo.Ternary(i.GetSubjectRelation() == "", "", InstanceSeparator+i.GetSubjectRelation())
}

func (i *relation) SubKey() string {
	return i.GetSubjectType() + TypeIDSeparator + i.GetSubjectId() +
		InstanceSeparator +
		i.GetRelation() +
		InstanceSeparator +
		i.GetObjectType() + TypeIDSeparator + i.GetObjectId() +
		lo.Ternary(i.GetSubjectRelation() == "", "", InstanceSeparator+i.GetSubjectRelation())
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
