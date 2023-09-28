package tests_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	dsm3 "github.com/aserto-dev/go-directory/aserto/directory/model/v3"
	"github.com/aserto-dev/go-directory/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/server"

	"github.com/gonvenience/ytbx"
	"github.com/homeport/dyff/pkg/dyff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

const blockSize = 1024 // test with 1KiB block size to exercise chunking.

func TestManifestV2(t *testing.T) {
	client, closer := testInit(t)
	t.Cleanup(closer)

	t.Run("set-manifest", testSetManifest(client, "./manifest_v2_test.yaml"))
	t.Run("get-manifest", testGetManifest(client, "./manifest_v2_test.yaml"))
	t.Run("delete-manifest", testDeleteManifest(client))
}

func TestManifestV3(t *testing.T) {
	client, closer := testInit(t)
	t.Cleanup(closer)

	t.Run("set-manifest", testSetManifest(client, "./manifest_v3_test.yaml"))
	t.Run("get-manifest", testGetManifest(client, "./manifest_v3_test.yaml"))
	t.Run("get-model", testGetModel(client))
	t.Run("delete-manifest", testDeleteManifest(client))
}

func testSetManifest(client *server.TestEdgeClient, manifest string) func(*testing.T) {
	return func(t *testing.T) {
		r, err := os.Open(manifest)
		require.NoError(t, err)

		stream, err := client.V3.Model.SetManifest(context.Background())
		require.NoError(t, err)

		buf := make([]byte, blockSize)
		for {
			n, err := r.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				assert.NoError(t, err)
			}

			if err := stream.Send(&dsm3.SetManifestRequest{
				Msg: &dsm3.SetManifestRequest_Body{
					Body: &dsm3.Body{Data: buf[0:n]},
				},
			}); err != nil {
				assert.NoError(t, err)
			}

			if n < blockSize {
				break
			}
		}

		if _, err := stream.CloseAndRecv(); err != nil {
			assert.NoError(t, err)
		}
	}
}

func testGetManifest(client *server.TestEdgeClient, manifest string) func(*testing.T) {
	return func(t *testing.T) {
		stream, err := client.V3.Model.GetManifest(context.Background(), &dsm3.GetManifestRequest{Empty: &emptypb.Empty{}})
		if err != nil {
			require.NoError(t, err)
		}

		data := bytes.Buffer{}

		bytesRecv := 0
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				assert.NoError(t, err)
			}

			if md, ok := resp.GetMsg().(*dsm3.GetManifestResponse_Metadata); ok {
				_ = md.Metadata
			}

			if body, ok := resp.GetMsg().(*dsm3.GetManifestResponse_Body); ok {
				data.Write(body.Body.Data)
				bytesRecv += len(body.Body.Data)
			}
		}

		tempManifest := path.Join(os.TempDir(), "manifest.yaml")
		if err := os.WriteFile(tempManifest, data.Bytes(), 0600); err != nil {
			require.NoError(t, err)
		}

		input1, err := ytbx.LoadFile(manifest)
		assert.NoError(t, err)

		input2, err := ytbx.LoadFile(tempManifest)
		assert.NoError(t, err)

		// compare
		opts := []dyff.CompareOption{dyff.IgnoreOrderChanges(true)}
		report, err := dyff.CompareInputFiles(input1, input2, opts...)
		assert.NoError(t, err)

		for _, diff := range report.Diffs {
			t.Logf(diff.Path.ToDotStyle())
		}
	}
}

func testGetModel(client *server.TestEdgeClient) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		hdr := metadata.New(map[string]string{"aserto-model-request": "model-only"})
		ctx = metadata.NewOutgoingContext(ctx, hdr)

		stream, err := client.V3.Model.GetManifest(ctx, &dsm3.GetManifestRequest{Empty: &emptypb.Empty{}})
		if err != nil {
			require.NoError(t, err)
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				assert.NoError(t, err)
			}

			if md, ok := resp.GetMsg().(*dsm3.GetManifestResponse_Metadata); ok {
				_ = md.Metadata
			}

			if body, ok := resp.GetMsg().(*dsm3.GetManifestResponse_Body); ok {
				_ = body
			}

			if model, ok := resp.GetMsg().(*dsm3.GetManifestResponse_Model); ok {
				buf := new(bytes.Buffer)
				if err := pb.ProtoToBuf(buf, model.Model); err != nil {
					assert.NoError(t, err)
				}

				tempModel := path.Join(os.TempDir(), "model.json")
				if err := os.WriteFile(tempModel, buf.Bytes(), 0600); err != nil {
					require.NoError(t, err)
				}

				fmt.Println(tempModel)
			}
		}
	}
}

func testDeleteManifest(client *server.TestEdgeClient) func(*testing.T) {
	return func(t *testing.T) {
		_, err := client.V3.Model.DeleteManifest(
			context.Background(),
			&dsm3.DeleteManifestRequest{Empty: &emptypb.Empty{}},
		)
		require.NoError(t, err)
	}
}
