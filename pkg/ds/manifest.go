package ds

import (
	"bytes"
	"context"
	"hash/fnv"
	"strconv"

	"github.com/aserto-dev/azm/model"
	dsm3 "github.com/aserto-dev/go-directory/aserto/directory/model/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	bolt "go.etcd.io/bbolt"
)

type manifest struct {
	Metadata *dsm3.Metadata
	Body     *dsm3.Body
}

func Manifest(metadata *dsm3.Metadata) *manifest {
	return &manifest{
		Metadata: metadata,
		Body:     &dsm3.Body{},
	}
}

// Get, hydrates the manifest from the _manifest bucket
// _metadata/{name}/{version}/metadata
// _metadata/{name}/{version}/body.
func (m *manifest) Get(ctx context.Context, tx *bolt.Tx) (*manifest, error) {
	if ok, _ := bdb.BucketExists(tx, bdb.ManifestPath); !ok {
		return nil, bdb.ErrPathNotFound
	}

	metadata, err := bdb.Get[dsm3.Metadata](ctx, tx, bdb.ManifestPath, bdb.MetadataKey)
	if err != nil {
		return nil, err
	}

	body, err := bdb.Get[dsm3.Body](ctx, tx, bdb.ManifestPath, bdb.BodyKey)
	if err != nil {
		return nil, err
	}

	return &manifest{Metadata: metadata, Body: body}, nil
}

// Get, hydrates the model cache from the _manifest
// _metadata/{name}/{version}/model.
func (m *manifest) GetModel(ctx context.Context, tx *bolt.Tx) (*model.Model, error) {
	if ok, _ := bdb.BucketExists(tx, bdb.ManifestPath); !ok {
		return nil, bdb.ErrPathNotFound
	}

	mod, err := bdb.GetAny[model.Model](ctx, tx, bdb.ManifestPath, bdb.ModelKey)
	if err != nil {
		return nil, err
	}

	return mod, nil
}

// Set, persists the manifest body in the _manifest bucket
// _metadata/{name}/{version}/metadata
// _metadata/{name}/{version}/body.
func (m *manifest) Set(ctx context.Context, tx *bolt.Tx, buf *bytes.Buffer) error {
	if _, err := bdb.CreateBucket(tx, bdb.ManifestPath); err != nil {
		return err
	}

	if _, err := bdb.Set[dsm3.Metadata](ctx, tx, bdb.ManifestPath, bdb.MetadataKey, m.Metadata); err != nil {
		return err
	}

	m.Body = &dsm3.Body{Data: buf.Bytes()}
	if _, err := bdb.Set[dsm3.Body](ctx, tx, bdb.ManifestPath, bdb.BodyKey, m.Body); err != nil {
		return err
	}

	return nil
}

// SetModel, persists the model cache in the _manifest bucket
// _metadata/{name}/{version}/model.
func (m *manifest) SetModel(ctx context.Context, tx *bolt.Tx, mod *model.Model) error {
	if _, err := bdb.SetAny[model.Model](ctx, tx, bdb.ManifestPath, bdb.ModelKey, mod); err != nil {
		return err
	}

	return nil
}

// Delete, removes the manifest from the _manifest bucket using key=name:version,
// if not version is provided, version will be set to latest.
func (m *manifest) Delete(ctx context.Context, tx *bolt.Tx) error {
	if ok, _ := bdb.BucketExists(tx, bdb.ManifestPath); !ok {
		return nil
	}

	if err := bdb.DeleteBucket(tx, bdb.ManifestPath); err != nil {
		return err
	}

	return nil
}

func (m *manifest) Hash() string {
	h := fnv.New64a()
	h.Reset()
	if _, err := h.Write(m.Body.Data); err != nil {
		return DefaultHash
	}
	return strconv.FormatUint(h.Sum64(), 10)
}