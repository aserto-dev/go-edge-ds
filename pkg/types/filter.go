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

	if req.Subject != nil && req.Subject.Id != nil && ID.IsValidIfSet(req.Subject.GetId()) {
		filters = append(filters, func(r *Relation) bool {
			return r.Subject.GetId() == req.Subject.GetId()
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

	if req.Object != nil && req.Object.Id != nil && ID.IsValidIfSet(req.Object.GetId()) {
		filters = append(filters, func(r *Relation) bool {
			return r.Object.GetId() == req.Object.GetId()
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
