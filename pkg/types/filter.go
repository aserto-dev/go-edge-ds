package types

import "strings"

func makeFilter(args ...string) string {
	filter := strings.Builder{}
	for _, v := range args {
		if strings.TrimSpace(v) == "" {
			break
		}
		filter.WriteString(v)
	}
	return filter.String()
}

type relationFilter func(*Relation) bool

func filterRelations(req *RelationIdentifier, relations []*Relation) []*Relation {
	filters := []relationFilter{}

	if req.Subject != nil && req.Subject.Key != nil && ID.IsValidIfSet(req.Subject.GetKey()) {
		filters = append(filters, func(r *Relation) bool {
			return r.Subject.GetKey() == req.Subject.GetKey()
		})
	}
	if req.Subject != nil && req.Subject.Type != nil && *req.Subject.Type != "" {
		filters = append(filters, func(r *Relation) bool {
			return strings.EqualFold(r.Subject.GetType(), req.Subject.GetType())
		})
	}

	if req.Relation != nil && req.Relation.Name != nil && *req.Relation.Name != "" {
		filters = append(filters, func(r *Relation) bool {
			return strings.EqualFold(r.Relation.GetRelation(), req.Relation.GetName())
		})
	}

	if req.Object != nil && req.Object.Key != nil && ID.IsValidIfSet(req.Object.GetKey()) {
		filters = append(filters, func(r *Relation) bool {
			return r.Object.GetKey() == req.Object.GetKey()
		})
	}
	if req.Object != nil && req.Object.Type != nil && *req.Object.Type != "" {
		filters = append(filters, func(r *Relation) bool {
			return strings.EqualFold(r.Object.GetType(), req.Object.GetType())
		})
	}

	results := []*Relation{}
	for i := 0; i < len(relations); i++ {
		if includeRelation(relations[i], filters) {
			results = append(results, relations[i])
		}
	}

	return results
}

func includeRelation(rel *Relation, filters []relationFilter) bool {
	for _, fn := range filters {
		if !fn(rel) {
			return false
		}
	}
	return true
}
