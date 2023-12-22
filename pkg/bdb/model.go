package bdb

import (
	"bytes"
	"encoding/json"

	"github.com/aserto-dev/azm/model"
	v3 "github.com/aserto-dev/azm/v3"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LoadModel, reads the serialized model from the store
// and swaps the model instance in the cache.Cache using
// cache.UpdateModel.
func (s *BoltDB) LoadModel() error {
	err := s.db.View(func(tx *bolt.Tx) error {
		if ok, _ := BucketExists(tx, ManifestPath); !ok {
			return nil
		}

		buf, version, err := readModel(tx)
		switch {
		case status.Code(err) == codes.NotFound:
			return nil
		case err != nil:
			return err
		}

		var mod *model.Model

		switch version {
		case model.ModelVersion:
			// The serialized model is on the latest version
			var m model.Model
			if err := json.Unmarshal(buf, &m); err != nil {
				return err
			}
			mod = &m
		default:
			// need to reload the model from the manifest
			mod, err = fromManifest(tx)
			if err != nil {
				return err
			}
		}

		if err := s.mc.UpdateModel(mod); err != nil {
			return err
		}

		return nil
	})

	return err
}

func readModel(tx *bolt.Tx) ([]byte, int, error) {
	buf, err := GetKey(tx, ManifestPath, ModelKey)
	if err != nil {
		return nil, 0, err
	}

	var m map[string]any
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil, 0, err
	}

	return buf, int(m["version"].(float64)), nil
}

func fromManifest(tx *bolt.Tx) (*model.Model, error) {
	manifest, err := GetKey(tx, ManifestPath, BodyKey)
	if err != nil {
		return nil, err
	}

	return v3.Load(bytes.NewReader(manifest))
}
