package tests_test

import (
	"testing"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestPermissions(t *testing.T) {
	tcs := []*TestCase{}

	tcs = append(tcs, permissionTestCasesWithID...)
	tcs = append(tcs, permissionTestCasesWithoutID...)
	tcs = append(tcs, permissionTestCasesStreamMode...)

	testRunner(t, tcs)
}

var permissionTestCasesWithID = []*TestCase{
	{
		Name: "create test-perm-1",
		Req: &dsw.SetPermissionRequest{
			Permission: &dsc.Permission{
				Name:        "permission-1",
				DisplayName: "Permission 1",
				Hash:        "",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetPermissionResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)

				assert.Equal(t, "permission-1", resp.Result.Name)
				assert.Equal(t, "Permission 1", resp.Result.DisplayName)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "7685029755427542192", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get test-perm-1",
		Req: &dsr.GetPermissionRequest{
			Param: &dsc.PermissionIdentifier{
				Name: proto.String("permission-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetPermissionResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)

				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "permission-1", resp.Result.Name)
				assert.Equal(t, "Permission 1", resp.Result.DisplayName)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "7685029755427542192", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "update test-perm-1",
		Req: &dsw.SetPermissionRequest{
			Permission: &dsc.Permission{
				Name:        "permission-1",
				DisplayName: "Permission 11",
				Hash:        "7685029755427542192",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetPermissionResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)

				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "permission-1", resp.Result.Name)
				assert.Equal(t, "Permission 11", resp.Result.DisplayName)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "14563374343076255539", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get updated test-perm-11",
		Req: &dsr.GetPermissionRequest{
			Param: &dsc.PermissionIdentifier{
				Name: proto.String("permission-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsr.GetPermissionResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)

				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "permission-1", resp.Result.Name)
				assert.Equal(t, "Permission 11", resp.Result.DisplayName)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.Equal(t, "14563374343076255539", resp.Result.Hash)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "delete test-perm-1",
		Req: &dsw.DeletePermissionRequest{
			Param: &dsc.PermissionIdentifier{
				Name: proto.String("permission-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.DeletePermissionResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "get deleted test-perm-1",
		Req: &dsr.GetPermissionRequest{
			Param: &dsc.PermissionIdentifier{
				Name: proto.String("permission-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			assert.Nil(t, msg)
			assert.Error(t, tErr)
			return func(proto.Message) {}
		},
	},
	{
		Name: "delete deleted test-perm-1 by id",
		Req: &dsw.DeletePermissionRequest{
			Param: &dsc.PermissionIdentifier{
				Name: proto.String("permission-1"),
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.DeletePermissionResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
			}
			return func(proto.Message) {}
		},
	},
}

var permissionTestCasesWithoutID = []*TestCase{
	{
		Name: "create test-perm with no-id",
		Req: &dsw.SetPermissionRequest{
			Permission: &dsc.Permission{
				Name:        "permission",
				DisplayName: "Permission",
				Hash:        "",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetPermissionResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "permission", resp.Result.Name)
				assert.Equal(t, "Permission", resp.Result.DisplayName)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.True(t, len(resp.Result.Hash) > 4)

				return func(req proto.Message) {
					lastHash := resp.Result.Hash

					switch r := req.(type) {
					case *dsw.SetPermissionRequest:
						r.Permission.Hash = lastHash
					}
				}
			}
			return func(proto.Message) {}
		},
	},
	{
		Name: "update test-perm with no-id",
		Req: &dsw.SetPermissionRequest{
			Permission: &dsc.Permission{
				Name:        "permission",
				DisplayName: "Permission NO-ID",
			},
		},
		Checks: func(t *testing.T, msg proto.Message, tErr error) func(proto.Message) {
			require.NotNil(t, msg)
			switch resp := msg.(type) {
			case *dsw.SetPermissionResponse:
				assert.NoError(t, tErr)
				assert.NotNil(t, resp)
				assert.NotNil(t, resp.Result)
				t.Logf("resp hash:%s", resp.Result.Hash)

				assert.Equal(t, "permission", resp.Result.Name)
				assert.Equal(t, "Permission NO-ID", resp.Result.DisplayName)
				assert.NotEmpty(t, resp.Result.Hash)
				assert.True(t, len(resp.Result.Hash) > 4)
			}
			return func(proto.Message) {}
		},
	},
}

var permissionTestCasesStreamMode = []*TestCase{}
