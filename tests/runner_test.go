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

	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
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
	if err := os.MkdirAll(dirPath, 0o700); err != nil {
		panic(err)
	}

	dbPath := path.Join(dirPath, "edge-ds", "test-eds.db")
	os.Remove(dbPath)
	fmt.Println(dbPath)

	cfg := directory.Config{
		DBPath:         dbPath,
		RequestTimeout: time.Second * 2,
		Seed:           true,
		EnableV2:       true,
	}

	client, closer = server.NewTestEdgeServer(ctx, &logger, &cfg)

	exitVal := m.Run()

	closer()
	os.Exit(exitVal)
}

func testInit() (*server.TestEdgeClient, func()) {
	return client, func() {}
}

func testRunner(t *testing.T, tcs []*TestCase) {
	client, cleanup := testInit()
	t.Cleanup(cleanup)

	ctx := context.Background()

	manifest, err := os.ReadFile("./manifest_v3_test.yaml")
	require.NoError(t, err)

	require.NoError(t, deleteManifest(client))
	require.NoError(t, setManifest(client, manifest))

	var apply func(proto.Message)

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			if apply != nil {
				apply(tc.Req)
			}
			runTestCase(ctx, t, tc)
		})
	}
}

func runTestCase(ctx context.Context, t *testing.T, tc *TestCase) func(proto.Message) {
	switch req := tc.Req.(type) {
	// V3
	///////////////////////////////////////////////////////////////
	case *dsr3.GetObjectRequest:
		resp, err := client.V3.Reader.GetObject(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsw3.SetObjectRequest:
		resp, err := client.V3.Writer.SetObject(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsw3.DeleteObjectRequest:
		resp, err := client.V3.Writer.DeleteObject(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsr3.GetRelationRequest:
		resp, err := client.V3.Reader.GetRelation(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsw3.SetRelationRequest:
		resp, err := client.V3.Writer.SetRelation(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsw3.DeleteRelationRequest:
		resp, err := client.V3.Writer.DeleteRelation(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsr3.GetRelationsRequest:
		resp, err := client.V3.Reader.GetRelations(ctx, req)
		return tc.Checks(t, resp, err)
	}

	return func(proto.Message) {}
}
