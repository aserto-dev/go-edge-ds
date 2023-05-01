package types

import (
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
)

type relationFilter func(*dsc.Relation) bool

func filterRelations(req *dsc.RelationIdentifier, relations []*dsc.Relation) []*dsc.Relation {
	filters := []relationFilter{}

	if req.Subject != nil && req.Subject.Key != nil {
		filters = append(filters, func(r *dsc.Relation) bool {
			return r.Subject.GetKey() == req.Subject.GetKey()
		})
	}
	if req.Subject != nil && req.Subject.Type != nil && *req.Subject.Type != "" {
		filters = append(filters, func(r *dsc.Relation) bool {
			return strings.EqualFold(r.Subject.GetType(), req.Subject.GetType())
		})
	}

	if req.Relation != nil && req.Relation.Name != nil && *req.Relation.Name != "" {
		filters = append(filters, func(r *dsc.Relation) bool {
			return strings.EqualFold(r.GetRelation(), req.Relation.GetName())
		})
	}

	if req.Object != nil && req.Object.Key != nil {
		filters = append(filters, func(r *dsc.Relation) bool {
			return r.Object.GetKey() == req.Object.GetKey()
		})
	}
	if req.Object != nil && req.Object.Type != nil && *req.Object.Type != "" {
		filters = append(filters, func(r *dsc.Relation) bool {
			return strings.EqualFold(r.Object.GetType(), req.Object.GetType())
		})
	}

	results := []*dsc.Relation{}
	for i := 0; i < len(relations); i++ {
		if includeRelation(relations[i], filters) {
			results = append(results, relations[i])
		}
	}

	return results
}

func includeRelation(rel *dsc.Relation, filters []relationFilter) bool {
	for _, fn := range filters {
		if !fn(rel) {
			return false
		}
	}
	return true
}
