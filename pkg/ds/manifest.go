package ds

import (
	"bytes"
	"context"
	"hash/fnv"
	"strconv"

	dsm3 "github.com/aserto-dev/go-directory/aserto/directory/model/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	bolt "go.etcd.io/bbolt"
)

const (
	manifestName    string = "default"
	manifestVersion string = "0"
	metadataKey     string = "metadata"
	bodyKey         string = "body"
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

// Get, hydrates the manifest from the _manifest bucket using key=name:version,
// if no version is provided, version will be set to latest.
func (m *manifest) Get(ctx context.Context, tx *bolt.Tx) (*manifest, error) {
	if ok, _ := bdb.BucketExists(tx, m.Path()); !ok {
		return nil, bdb.ErrPathNotFound
	}

	metadata, err := bdb.Get[dsm3.Metadata](ctx, tx, m.Path(), metadataKey)
	if err != nil {
		return nil, err
	}

	body, err := bdb.Get[dsm3.Body](ctx, tx, m.Path(), bodyKey)
	if err != nil {
		return nil, err
	}

	return &manifest{Metadata: metadata, Body: body}, nil
}

// Set, persists the manifest body in the _manifest bucket using key=name:version value=body
// if no version is provide, version will be set to latest.
// _metadata/{name}/{version}/metadata
// _metadata/{name}/{version}/body.
func (m *manifest) Set(ctx context.Context, tx *bolt.Tx, buf *bytes.Buffer) error {
	if _, err := bdb.CreateBucket(tx, m.Path()); err != nil {
		return err
	}

	if _, err := bdb.Set[dsm3.Metadata](ctx, tx, m.Path(), metadataKey, m.Metadata); err != nil {
		return err
	}

	m.Body = &dsm3.Body{Data: buf.Bytes()}
	if _, err := bdb.Set[dsm3.Body](ctx, tx, m.Path(), bodyKey, m.Body); err != nil {
		return err
	}

	return nil
}

// Delete, removes the manifest from the _manifest bucket using key=name:version,
// if not version is provided, version will be set to latest.
func (m *manifest) Delete(ctx context.Context, tx *bolt.Tx) error {
	if ok, _ := bdb.BucketExists(tx, m.Path()); !ok {
		return nil
	}

	if err := bdb.DeleteBucket(tx, m.Path()); err != nil {
		return err
	}

	return nil
}

// List, returns the metadata of each manifest instance.
func (m *manifest) List(ctx context.Context, tx *bolt.Tx) ([]*dsm3.Metadata, error) {
	results := []*dsm3.Metadata{}

	manifestNames, err := bdb.ListBuckets(tx, bdb.ManifestPath)
	if err != nil {
		return []*dsm3.Metadata{}, err
	}

	for _, manifest := range manifestNames {
		manifestVersions, err := bdb.ListBuckets(tx, append(bdb.ManifestPath, manifest))
		if err != nil {
			return []*dsm3.Metadata{}, err
		}

		for _, version := range manifestVersions {
			metadata, err := bdb.Get[dsm3.Metadata](ctx, tx, append(bdb.ManifestPath, manifest, version), metadataKey)
			if err != nil {
				return []*dsm3.Metadata{}, err
			}
			results = append(results, metadata)
		}
	}

	return results, nil
}

func (m *manifest) Path() bdb.Path {
	return append(bdb.ManifestPath, manifestName, manifestVersion)
}

func (m *manifest) Hash() string {
	h := fnv.New64a()
	h.Reset()
	if _, err := h.Write(m.Body.Data); err != nil {
		return DefaultHash
	}
	return strconv.FormatUint(h.Sum64(), 10)
}
