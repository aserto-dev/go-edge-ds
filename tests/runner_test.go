// nolint
package tests_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"testing"
	"time"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/directory"
	"github.com/aserto-dev/go-edge-ds/pkg/server"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type TestCase struct {
	Name   string
	Req    proto.Message
	Checks func(*testing.T, proto.Message, error) func(proto.Message)
}

var (
	client *server.TestEdgeClient
	closer func()
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	logger := zerolog.New(io.Discard)

	dirPath := os.TempDir()
	if err := os.MkdirAll(dirPath, 0700); err != nil {
		panic(err)
	}

	dbPath := path.Join(dirPath, "edge-ds", "test-eds.db")
	os.Remove(dbPath)
	fmt.Println(dbPath)

	cfg := directory.Config{
		DBPath:         dbPath,
		RequestTimeout: time.Second * 2,
		Seed:           true,
	}

	client, closer = server.NewTestEdgeServer(ctx, &logger, &cfg)

	exitVal := m.Run()

	closer()
	os.Exit(exitVal)
}

func TestGetObjectTypes(t *testing.T) {
	client, closer := testInit(t)
	t.Cleanup(closer)

	resp, err := client.V2.Reader.GetObjectTypes(context.Background(), &dsr2.GetObjectTypesRequest{})
	require.NoError(t, err)
	for _, v := range resp.Results {
		t.Logf("object_type: %s", v.Name)
	}
}

func TestGetRelationTypes(t *testing.T) {
	client, closer := testInit(t)
	t.Cleanup(closer)

	resp, err := client.V2.Reader.GetRelationTypes(context.Background(), &dsr2.GetRelationTypesRequest{
		Param: &dsc2.ObjectTypeIdentifier{},
		Page:  &dsc2.PaginationRequest{},
	})
	require.NoError(t, err)
	for _, v := range resp.Results {
		t.Logf("relation_type: %s:%s", v.ObjectType, v.Name)
	}
}

func TestGetPermissions(t *testing.T) {
	client, closer := testInit(t)
	t.Cleanup(closer)

	resp, err := client.V2.Reader.GetPermissions(context.Background(), &dsr2.GetPermissionsRequest{})
	require.NoError(t, err)
	for _, v := range resp.Results {
		t.Logf("permission: %s", v.Name)
	}
}

func testInit(t *testing.T) (*server.TestEdgeClient, func()) {
	return client, func() {}
}

func testRunner(t *testing.T, tcs []*TestCase) {
	client, cleanup := testInit(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	manifest, err := os.ReadFile("./manifest_v3_test.yaml")
	require.NoError(t, err)

	require.NoError(t, deleteManifest(client))
	require.NoError(t, setManifest(client, manifest))

	var apply func(proto.Message)

	for _, tc := range tcs {
		t.Logf("%s", tc.Name)

		switch req := tc.Req.(type) {
		case *dsr2.GetObjectRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Reader.GetObject(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.SetObjectRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.Object.Hash)
			}

			resp, err := client.V2.Writer.SetObject(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.DeleteObjectRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Writer.DeleteObject(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsr2.GetObjectTypeRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Reader.GetObjectType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.SetObjectTypeRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.ObjectType.Hash)
			}

			resp, err := client.V2.Writer.SetObjectType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.DeleteObjectTypeRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Writer.DeleteObjectType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsr2.GetPermissionRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Reader.GetPermission(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.SetPermissionRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.Permission.Hash)
			}

			resp, err := client.V2.Writer.SetPermission(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.DeletePermissionRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Writer.DeletePermission(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsr2.GetRelationRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Reader.GetRelation(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.SetRelationRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.Relation.Hash)
			}

			resp, err := client.V2.Writer.SetRelation(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.DeleteRelationRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Writer.DeleteRelation(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsr2.GetRelationTypeRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Reader.GetRelationType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.SetRelationTypeRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.RelationType.Hash)
			}

			resp, err := client.V2.Writer.SetRelationType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw2.DeleteRelationTypeRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.V2.Writer.DeleteRelationType(ctx, req)
			apply = tc.Checks(t, resp, err)
		}
	}
}
