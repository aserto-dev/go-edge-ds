package ds

import (
	"context"
	"fmt"
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

func ResolvePermission(ctx context.Context, tx *bolt.Tx, objectType, permission string) ([]string, error) {
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

	rt := relTypes[relTypeIndex[permission]]
	if rt.ObjectType != objectType || !contains(rt.Permissions, permission) {
		return relations, errors.Wrapf(ErrRelationTypeNotFound, "%s#%s", objectType, permission)
	}

	for _, r := range relTypes {
		if contains(r.Unions, permission) {
			relations = append(relations, r.Name)
		}
	}

	return relations, nil
}

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
	if rt.ObjectType != objectType || !contains(rt.Unions, relation) {
		return relations, errors.Wrapf(ErrRelationTypeNotFound, "%s#%s", objectType, relation)
	}

	for _, r := range relTypes {
		if contains(r.Unions, relation) {
			relations = append(relations, r.Name)
		}
	}

	return relations, nil
}

func contains(c []string, v string) bool {
	for i := 0; i < len(c); i++ {
		if strings.EqualFold(c[i], v) {
			return true
		}
	}
	return false
}
