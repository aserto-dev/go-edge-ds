package tests_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/directory"
	"github.com/aserto-dev/go-edge-ds/pkg/fs"
	"github.com/aserto-dev/go-edge-ds/pkg/server"
	"github.com/pkg/errors"

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
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	logger := zerolog.New(io.Discard)

	ctx, cancel := context.WithCancel(context.Background())

	dirPath := os.TempDir()
	if err := os.MkdirAll(dirPath, fs.FileModeOwnerRWX); err != nil {
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
	cancel()

	os.Exit(exitVal)
}

func importFile(stream dsw.Writer_ImportClient, file string) error {
	r, err := os.Open(file)
	if err != nil {
		return errors.Wrapf(err, "failed to open file: [%s]", file)
	}
	defer r.Close()

	reader, err := NewReader(r)
	if err != nil || reader == nil {
		fmt.Fprintf(os.Stderr, "Skipping file [%s]: [%s]\n", file, err.Error())
		return nil
	}
	defer reader.Close()

	objectType := reader.GetObjectType()
	switch objectType {
	case ObjectsStr:
		if err := loadObjects(stream, reader); err != nil {
			return err
		}

	case RelationsStr:
		if err := loadRelations(stream, reader); err != nil {
			return err
		}

	default:
		fmt.Fprintf(os.Stderr, "skipping file [%s] with object type [%s]\n", file, objectType)
	}

	return nil
}

func loadObjects(stream dsw.Writer_ImportClient, objects *Reader) error {
	defer objects.Close()

	var m dsc.Object

	for {
		err := objects.Read(&m)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			if strings.Contains(err.Error(), "unknown field") {
				continue
			}

			return err
		}

		if err := stream.Send(&dsw.ImportRequest{
			OpCode: dsw.Opcode_OPCODE_SET,
			Msg: &dsw.ImportRequest_Object{
				Object: &m,
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func loadRelations(stream dsw.Writer_ImportClient, relations *Reader) error {
	defer relations.Close()

	var m dsc.Relation

	for {
		err := relations.Read(&m)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			if strings.Contains(err.Error(), "unknown field") {
				continue
			}

			return err
		}

		if err := stream.Send(&dsw.ImportRequest{
			OpCode: dsw.Opcode_OPCODE_SET,
			Msg: &dsw.ImportRequest_Relation{
				Relation: &m,
			},
		}); err != nil {
			return err
		}
	}

	return nil
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
	case *dsr.GetObjectRequest:
		resp, err := client.V3.Reader.GetObject(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsw.SetObjectRequest:
		resp, err := client.V3.Writer.SetObject(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsw.DeleteObjectRequest:
		resp, err := client.V3.Writer.DeleteObject(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsr.GetRelationRequest:
		resp, err := client.V3.Reader.GetRelation(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsw.SetRelationRequest:
		resp, err := client.V3.Writer.SetRelation(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsw.DeleteRelationRequest:
		resp, err := client.V3.Writer.DeleteRelation(ctx, req)
		return tc.Checks(t, resp, err)

	case *dsr.GetRelationsRequest:
		resp, err := client.V3.Reader.GetRelations(ctx, req)
		return tc.Checks(t, resp, err)
	}

	return func(proto.Message) {}
}
