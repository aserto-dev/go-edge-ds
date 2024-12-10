package tests_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

	dsi3 "github.com/aserto-dev/go-directory/aserto/directory/importer/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func BenchmarkCheck(b *testing.B) {
	assert := require.New(b)

	checks, err := loadChecks()
	assert.NoError(err)
	assert.NotEmpty(checks)

	client, cleanup := testInit()
	b.Cleanup(cleanup)

	ctx := context.Background()

	manifest, err := os.ReadFile("./data/check/manifest.yaml")
	assert.NoError(err)

	assert.NoError(deleteManifest(client))
	assert.NoError(setManifest(client, manifest))

	g, iCtx := errgroup.WithContext(ctx)
	stream, err := client.V3.Importer.Import(iCtx)
	assert.NoError(err)

	g.Go(receiver(stream))

	assert.NoError(importFile(stream, "./data/check/objects.json"))
	assert.NoError(importFile(stream, "./data/check/relations.json"))
	assert.NoError(stream.CloseSend())

	assert.NoError(g.Wait())

	b.ResetTimer()

	for _, check := range checks {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := client.V3.Reader.Check(ctx, check)
				assert.NoError(err)
			}
		})
	}
}

func loadChecks() ([]*dsr3.CheckRequest, error) {
	bin, err := os.ReadFile("./data/check/check.json")
	if err != nil {
		return nil, err
	}

	var checks []*dsr3.CheckRequest
	if err := json.Unmarshal(bin, &checks); err != nil {
		return nil, err

	}

	return checks, nil
}

func receiver(stream dsi3.Importer_ImportClient) func() error {
	return func() error {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return nil
			}

			if err != nil {
				return err
			}
		}
	}
}
