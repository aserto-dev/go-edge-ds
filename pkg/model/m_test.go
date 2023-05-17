package model_test

import (
	"fmt"
	"strings"
	"testing"
)

type Model1 struct {
	ObjectTypes map[string]struct {
		RelationTypes map[string]struct {
			Unions      []string `json:"unions,omitempty" yaml:"unions,omitempty"`
			Permissions []string `json:"permissions,omitempty" yaml:"permissions,omitempty"`
		} `json:"relations,omitempty" yaml:"relations,omitempty"`
	} `json:"object_types,omitempty" yaml:"object_types,omitempty"`
}

type Model2 struct {
	ObjectTypes map[string]RelationTypes2 `json:"object_types,omitempty" yaml:"object_types,omitempty"`
}

type RelationTypes2 struct {
	RelationsTypes map[string]RelationType2 `json:"relations,omitempty" yaml:"relations,omitempty"`
}

type RelationType2 struct {
	Unions      []string `json:"unions,omitempty" yaml:"unions,omitempty"`
	Permissions []string `json:"permissions,omitempty" yaml:"permissions,omitempty"`
}

func TestResolve(t *testing.T) {
	m2 := Model2{
		ObjectTypes: map[string]RelationTypes2{
			"engine": {
				RelationsTypes: map[string]RelationType2{
					"owner": {
						Unions: []string{
							"contributor",
							"delete",
						},
					},
					"contributor": {
						Unions: []string{
							"reader",
							"update",
						},
					},
					"reader": {
						Unions: []string{
							"read",
						},
					},
					"read": {
						Permissions: []string{
							"can_read",
						},
					},
					"update": {
						Permissions: []string{
							"can_update",
						},
					},
					"delete": {
						Permissions: []string{
							"can_delete",
						},
					},
				},
			},
		},
	}
	_ = m2

	m3 := Model2{
		ObjectTypes: map[string]RelationTypes2{
			"engine": {
				RelationsTypes: map[string]RelationType2{
					"owner": {
						Unions: []string{
							"contributor",
							// "delete",
						},
						Permissions: []string{
							"delete",
						},
					},
					"contributor": {
						Unions: []string{
							"reader",
							// "update",
						},
						Permissions: []string{
							"update",
						},
					},
					"reader": {
						Unions: []string{
							// "read",
						},
						Permissions: []string{
							"read",
						},
					},
					// "read": {
					// 	Permissions: []string{
					// 		"read",
					// 	},
					// },
					// "update": {
					// 	Permissions: []string{
					// 		"update",
					// 	},
					// },
					// "delete": {
					// 	Permissions: []string{
					// 		"delete",
					// 	},
					// },
				},
			},
		},
	}
	_ = m3

	m := m2

	{
		fmt.Println("relation map")
		walker := NewRelationWalker(&m)
		for otn, ot := range m.ObjectTypes {
			for rtn := range ot.RelationsTypes {
				walker.Walk(otn, rtn)
				fmt.Printf("%s#%s -> %v\n", otn, rtn, strings.Join(walker.Results(), ", "))
				walker.Reset()
			}
		}
	}
	fmt.Println()
	{
		fmt.Println("permission map")
		walker := NewRelationWalker(&m)
		for otn, ot := range m.ObjectTypes {
			for rtn, rt := range ot.RelationsTypes {
				for _, p := range rt.Permissions {
					walker.Resolve(otn, rtn)
					fmt.Printf("%s#%s -> %v\n", otn, p, strings.Join(walker.Results(), ", "))
					walker.Reset()
				}
			}
		}
	}
}

// type PermissionWalker struct {
// 	m       *Model2
// 	results []string
// }

// func NewPermissionWalker(m *Model2) *PermissionWalker {
// 	return &PermissionWalker{
// 		m:       m,
// 		results: []string{},
// 	}
// }

// func (rw *PermissionWalker) Walk(ot, rt string) {
// 	for _, p := range rw.m.ObjectTypes[ot].RelationsTypes[rt].Permissions {
// 		rw.results = append(rw.results, u)
// 		rw.Walk(ot, u)
// 	}
// }

// func (rw *PermissionWalker) Results() []string {
// 	return rw.results
// }

// func (rw *PermissionWalker) Reset() {
// 	rw.results = []string{}
// }

type RelationWalker struct {
	m       *Model2
	results []string
}

func NewRelationWalker(m *Model2) *RelationWalker {
	return &RelationWalker{
		m:       m,
		results: []string{},
	}
}

func (rw *RelationWalker) Walk(ot, rt string) {
	for _, u := range rw.m.ObjectTypes[ot].RelationsTypes[rt].Unions {
		rw.results = append(rw.results, u)
		rw.Walk(ot, u)
	}
}

func (rw *RelationWalker) Resolve(ot, rt string) {
	for _, r := range rw.m.ObjectTypes[ot].RelationsTypes {
		for _, u := range r.Unions {
			if u == rt {
				rw.results = append(rw.results, u)
			}
		}
	}
}

func (rw *RelationWalker) Results() []string {
	return rw.results
}

func (rw *RelationWalker) Reset() {
	rw.results = []string{}
}
