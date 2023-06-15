package ds

import (
	"context"
	"fmt"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
)

// ResolvePermission, resolves the permission to covering relations, within the scope of the object_type.
func ResolvePermission(ctx context.Context, tx *bolt.Tx, objectType, permission string) ([]string, error) {
	relations := []string{}

	filter := fmt.Sprintf("%s:", objectType)

	// relTypes contains the collection of relations for a the object_type.
	relTypes, err := bdb.Scan[dsc.RelationType](ctx, tx, bdb.RelationTypesPath, filter)
	if err != nil {
		return relations, err
	}

	// determine which relation(s) cover the permission.
	for _, r := range relTypes {
		if lo.Contains(r.Permissions, permission) {
			relations = append(relations, r.Name)
		}
	}

	// expand relation, to include covering relations.
	for i := 0; i < len(relations); i++ {
		relations = append(relations, ExpandRelation(relTypes, relations[i])...)
	}

	return lo.Uniq(relations), nil
}

// ExpandRelation, expand relation to include covering relation within the object_type.
func ExpandRelation(relTypes []*dsc.RelationType, relation string) []string {
	res := []string{}

	for i := range relTypes {
		for _, u := range relTypes[i].Unions {
			if u == relation {
				res = append(res, relTypes[i].Name)
			}
		}
	}

	for i := 0; i < len(res); i++ {
		res = append(res, ExpandRelation(relTypes, res[i])...)
	}

	return res
}

// ResolveRelation, resolves the relation to covering relations within the scope of the object_type.
func ResolveRelation(ctx context.Context, tx *bolt.Tx, objectType, relation string) ([]string, error) {
	relations := []string{}

	filter := fmt.Sprintf("%s:", objectType)
	relTypes, err := bdb.Scan[dsc.RelationType](ctx, tx, bdb.RelationTypesPath, filter)
	if err != nil {
		return relations, err
	}

	relTypeIndex := map[string]int{}
	for i, r := range relTypes {
		relTypeIndex[r.Name] = i
	}

	rt := relTypes[relTypeIndex[relation]]
	if rt.ObjectType != objectType && !lo.Contains(rt.Unions, relation) {
		return relations, errors.Wrapf(ErrRelationTypeNotFound, "%s#%s", objectType, relation)
	}

	for _, r := range relTypes {
		if lo.Contains(r.Unions, relation) {
			relations = append(relations, r.Name)
		}
	}

	return relations, nil
}
