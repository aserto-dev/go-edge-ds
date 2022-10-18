//nolint
package tests_test

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"

	"google.golang.org/protobuf/proto"

	"github.com/aserto-dev/edge-ds/pkg/directory"
	"github.com/aserto-dev/edge-ds/pkg/server"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
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
	cfg := directory.Config{
		DBPath:         "test-eds.db",
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

	resp, err := client.Reader.GetObjectTypes(context.Background(), &dsr.GetObjectTypesRequest{})
	assert.NoError(t, err)
	for _, v := range resp.Results {
		t.Logf("%d %s", v.Id, v.Name)
	}
}

func TestGetRelationTypes(t *testing.T) {
	client, closer := testInit(t)
	t.Cleanup(closer)

	resp, err := client.Reader.GetRelationTypes(context.Background(), &dsr.GetRelationTypesRequest{})
	assert.NoError(t, err)
	for _, v := range resp.Results {
		t.Logf("%d %s %s", v.Id, v.Name, v.ObjectType)
	}
}

func TestGetPermissions(t *testing.T) {
	client, closer := testInit(t)
	t.Cleanup(closer)

	resp, err := client.Reader.GetPermissions(context.Background(), &dsr.GetPermissionsRequest{})
	assert.NoError(t, err)
	for _, v := range resp.Results {
		t.Logf("%s %s", v.Id, v.Name)
	}
}

func testInit(t *testing.T) (*server.TestEdgeClient, func()) {
	return client, func() {}
}

func testRunner(t *testing.T, tcs []*TestCase) {
	client, cleanup := testInit(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	var apply func(proto.Message)

	for _, tc := range tcs {
		t.Logf("%s", tc.Name)

		switch req := tc.Req.(type) {
		case *dsr.GetObjectRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Reader.GetObject(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.SetObjectRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.Object.Hash)
			}

			resp, err := client.Writer.SetObject(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.DeleteObjectRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Writer.DeleteObject(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsr.GetObjectTypeRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Reader.GetObjectType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.SetObjectTypeRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.ObjectType.Hash)
			}

			resp, err := client.Writer.SetObjectType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.DeleteObjectTypeRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Writer.DeleteObjectType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsr.GetPermissionRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Reader.GetPermission(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.SetPermissionRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.Permission.Hash)
			}

			resp, err := client.Writer.SetPermission(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.DeletePermissionRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Writer.DeletePermission(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsr.GetRelationRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Reader.GetRelation(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.SetRelationRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.Relation.Hash)
			}

			resp, err := client.Writer.SetRelation(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.DeleteRelationRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Writer.DeleteRelation(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsr.GetRelationTypeRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Reader.GetRelationType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.SetRelationTypeRequest:
			if apply != nil {
				apply(req)
				t.Logf("propagated hash:%s", req.RelationType.Hash)
			}

			resp, err := client.Writer.SetRelationType(ctx, req)
			apply = tc.Checks(t, resp, err)

		case *dsw.DeleteRelationTypeRequest:
			if apply != nil {
				apply(req)
			}

			resp, err := client.Writer.DeleteRelationType(ctx, req)
			apply = tc.Checks(t, resp, err)
		}
	}
}
