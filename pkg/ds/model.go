package ds

import (
	"context"
	"fmt"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
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
	relTypes, err := bdb.Scan[dsc2.RelationType](ctx, tx, bdb.RelationTypesPath, filter)
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
func ExpandRelation(relTypes []*dsc2.RelationType, relation string) []string {
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
	relations := []string{relation}

	filter := fmt.Sprintf("%s:", objectType)
	relTypes, err := bdb.Scan[dsc2.RelationType](ctx, tx, bdb.RelationTypesPath, filter)
	if err != nil {
		return relations, err
	}

	relTypeIndex := map[string]int{}
	for i, r := range relTypes {
		relTypeIndex[r.Name] = i
	}

	index, ok := relTypeIndex[relation]
	if !ok {
		return relations, errors.Wrapf(ErrRelationTypeNotFound, "%s#%s", objectType, relation)
	}

	rt := relTypes[index]
	if rt.ObjectType != objectType && !lo.Contains(rt.Unions, relation) {
		return relations, errors.Wrapf(ErrRelationTypeNotFound, "%s#%s", objectType, relation)
	}

	for _, r := range relTypes {
		if lo.Contains(r.Unions, relation) {
			relations = append(relations, r.Name)
		}
	}

	return lo.Uniq(relations), nil
}

// CreateUserSet, create the computed user set of a subject.
func CreateUserSet(ctx context.Context, tx *bolt.Tx, subject *dsc2.ObjectIdentifier) ([]*dsc2.ObjectIdentifier, error) {
	result := []*dsc2.ObjectIdentifier{subject}

	filter := ObjectIdentifier(subject).Key() + InstanceSeparator + "member"
	relations, err := bdb.Scan[dsc2.Relation](ctx, tx, bdb.RelationsSubPath, filter)
	if err != nil {
		return nil, err
	}

	for _, r := range relations {
		if r.Object.GetType() == "group" {
			result = append(result, r.Object)
		}
	}

	return result, nil
}
