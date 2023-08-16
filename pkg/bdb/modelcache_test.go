package bdb_test

import (
	"encoding/json"
	"os"
	"testing"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadModelCache will fail the test if the load fails.
func loadModelCache(t *testing.T) *bdb.ModelCache {
	r, err := os.Open("./modelcache_test.json")
	require.NoError(t, err)

	var mc bdb.ModelCache
	dec := json.NewDecoder(r)
	if err := dec.Decode(&mc); err != nil {
		require.NoError(t, err)
	}
	return &mc
}

func TestExpandRelation(t *testing.T) {
	mc := loadModelCache(t)

	// tenant:directory-reader does not exist, results should be an empty array.
	relations := mc.ExpandRelation("tenant", "directory-reader")
	assert.Len(t, relations, 0)

	// system:directory-store-writer is not union-ed with any other relations,
	// results should be an single element array, of the requested relation.
	relations = mc.ExpandRelation("system", "directory-store-writer")
	assert.Len(t, relations, 1)
	assert.Contains(t, relations, "directory-store-writer")

	// tenant:directory-client-reader is union-ed by directory-client-writer.
	relations = mc.ExpandRelation("tenant", "directory-client-reader")
	assert.Len(t, relations, 2)
	assert.Contains(t, relations, "directory-client-reader")
	assert.Contains(t, relations, "directory-client-writer")

	// tenant:viewer is union-ed by owner, admin, member.
	relations = mc.ExpandRelation("tenant", "viewer")
	assert.Len(t, relations, 4)
	assert.Contains(t, relations, "viewer")
	assert.Contains(t, relations, "owner")
	assert.Contains(t, relations, "admin")
	assert.Contains(t, relations, "member")

	// tenant:member is union-ed by owner, admin.
	relations = mc.ExpandRelation("tenant", "member")
	assert.Len(t, relations, 3)
	assert.Contains(t, relations, "owner")
	assert.Contains(t, relations, "admin")
	assert.Contains(t, relations, "member")

	// tenant:admin is union-ed by owner.
	relations = mc.ExpandRelation("tenant", "admin")
	assert.Len(t, relations, 2)
	assert.Contains(t, relations, "owner")
	assert.Contains(t, relations, "admin")

	// tenant:owner is not union-ed by any other relation
	relations = mc.ExpandRelation("tenant", "owner")
	assert.Len(t, relations, 1)
	assert.Contains(t, relations, "owner")

	// tenant:none-relation none-relation is a none existing relation
	relations = mc.ExpandRelation("tenant", "non-relation")
	assert.Len(t, relations, 0)

	// none-tenant:none-relation, none-tenant is a none existing tenant
	relations = mc.ExpandRelation("none-tenant", "non-relation")
	assert.Len(t, relations, 0)
}

func TestResolvePermission(t *testing.T) {
	mc := loadModelCache(t)

	relations := mc.ExpandPermission("tenant", "none-permission")
	assert.Len(t, relations, 0)

	relations = mc.ExpandPermission("none-tenant", "none-permission")
	assert.Len(t, relations, 0)

	relations = mc.ExpandPermission("tenant", "aserto.directory.writer.v2.Writer.SetObject")
	assert.Len(t, relations, 3)
	assert.Contains(t, relations, "owner")
	assert.Contains(t, relations, "admin")
	assert.Contains(t, relations, "directory-client-writer")

	relations = mc.ExpandPermission("tenant", "aserto.directory.reader.v2.Reader.GetObject")
	assert.Len(t, relations, 6)
	assert.Contains(t, relations, "owner")
	assert.Contains(t, relations, "admin")
	assert.Contains(t, relations, "member")
	assert.Contains(t, relations, "viewer")
	assert.Contains(t, relations, "directory-client-reader")
	assert.Contains(t, relations, "directory-client-writer")

	relations = mc.ExpandPermission("tenant", "aserto.tenant.onboarding.v1.Onboarding.ClaimTenant")
	assert.Len(t, relations, 1)
	assert.Contains(t, relations, "owner")
}

func TestGetObjectTypeV2(t *testing.T) {
	mc := loadModelCache(t)

	objectType, err := mc.GetObjectType("")
	assert.Error(t, err, derr.ErrObjectTypeNotFound)
	assert.Equal(t, &dsc2.ObjectType{}, objectType)

	objectType, err = mc.GetObjectType("foobar")
	assert.Error(t, err, derr.ErrObjectTypeNotFound)
	assert.Equal(t, &dsc2.ObjectType{}, objectType)

	objectType, err = mc.GetObjectType("tenant")
	assert.NoError(t, err)
	assert.Equal(t, objectType.Name, "tenant")
}

func TestGetObjectTypesV2(t *testing.T) {
	mc := loadModelCache(t)

	objectTypes, err := mc.GetObjectTypes()
	assert.NoError(t, err)
	assert.Len(t, objectTypes, 11)
}

func TestGetRelationTypeV2(t *testing.T) {
	mc := loadModelCache(t)

	relationType, err := mc.GetRelationType("", "")
	assert.Error(t, err, derr.ErrRelationTypeNotFound)
	assert.Equal(t, &dsc2.RelationType{}, relationType)

	relationType, err = mc.GetRelationType("tenant", "")
	assert.Error(t, err, derr.ErrObjectTypeNotFound)
	assert.Equal(t, &dsc2.RelationType{}, relationType)

	relationType, err = mc.GetRelationType("tenant", "admin")
	assert.NoError(t, err)
	assert.Equal(t, relationType.ObjectType, "tenant")
	assert.Equal(t, relationType.Name, "admin")
	assert.Len(t, relationType.Unions, 2)
	assert.Len(t, relationType.Permissions, 171)
}

func TestGetRelationTypesV2(t *testing.T) {
	mc := loadModelCache(t)

	relationTypes, err := mc.GetRelationTypes("foobar")
	assert.Error(t, err, derr.ErrObjectTypeNotFound)
	assert.Len(t, relationTypes, 0)

	relationTypes, err = mc.GetRelationTypes("")
	assert.NoError(t, err)
	assert.Len(t, relationTypes, 23)

	relationTypes, err = mc.GetRelationTypes("tenant")
	assert.NoError(t, err)
	assert.Len(t, relationTypes, 10)
}

func TestGetPermissionV2(t *testing.T) {
	mc := loadModelCache(t)

	permission, err := mc.GetPermission("")
	assert.Error(t, err, derr.ErrPermissionNotFound)
	assert.Equal(t, &dsc2.Permission{}, permission)

	permission, err = mc.GetPermission("foo.bar")
	assert.Error(t, err, derr.ErrPermissionNotFound)
	assert.Equal(t, &dsc2.Permission{}, permission)

	permission, err = mc.GetPermission("aserto.discovery.policy.v2.Discovery.OPAInstanceDiscovery")
	assert.NoError(t, err)
	assert.Equal(t, permission.Name, "aserto.discovery.policy.v2.Discovery.OPAInstanceDiscovery")
}

func TestGetPermissionsV2(t *testing.T) {
	mc := loadModelCache(t)

	permissions, err := mc.GetPermissions()
	assert.NoError(t, err)
	assert.Len(t, permissions, 233)
}
