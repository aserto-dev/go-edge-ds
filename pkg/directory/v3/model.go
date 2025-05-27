package v3

import (
	"bytes"
	"context"

	"github.com/aserto-dev/azm/model"
	v3 "github.com/aserto-dev/azm/v3"
	"github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	"github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-directory/pkg/pb"
	"github.com/aserto-dev/go-directory/pkg/validator"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *Reader) GetManifest(ctx context.Context, req *reader.GetManifestRequest) (*reader.GetManifestResponse, error) {
	resp := &reader.GetManifestResponse{}

	if err := validator.GetManifestRequest(req); err != nil {
		return nil, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		result, err := s.getManifest(ctx, tx)
		if err != nil {
			return err
		}

		resp.Manifest = result

		return nil
	})

	return resp, err
}

func (s *Reader) getManifest(ctx context.Context, tx *bolt.Tx) (*common.Manifest, error) {
	resp := &common.Manifest{}

	manifest, err := bdb.Get[common.Manifest](ctx, tx, bdb.ManifestPathV2, bdb.ManifestKey)

	switch {
	case status.Code(err) == codes.NotFound:
		return resp, nil

	case err != nil:
		return resp, errors.Errorf("failed to get manifest")

	default:
		resp = manifest
		return resp, nil
	}
}

func (s *Reader) GetModel(ctx context.Context, req *reader.GetModelRequest) (*reader.GetModelResponse, error) {
	resp := &reader.GetModelResponse{}

	if err := validator.GetModelRequest(req); err != nil {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		result, err := s.getModel(ctx, tx)
		if err != nil {
			return err
		}

		resp.Model = result

		return nil
	})

	return resp, err
}

func (s *Reader) getModel(ctx context.Context, tx *bolt.Tx) (*common.Model, error) {
	resp := &common.Model{}

	model, err := bdb.Get[common.Model](ctx, tx, bdb.ManifestPathV2, bdb.ModelKey)

	switch {
	case status.Code(err) == codes.NotFound:
		return resp, nil

	case err != nil:
		return resp, errors.Errorf("failed to get model")

	default:
		resp = model
		return resp, nil
	}
}

func (s *Writer) SetManifest(ctx context.Context, req *writer.SetManifestRequest) (*writer.SetManifestResponse, error) {
	resp := &writer.SetManifestResponse{}

	if err := validator.SetManifestRequest(req); err != nil {
		return nil, err
	}

	if err := validator.Manifest(req.GetManifest()); err != nil {
		return nil, err
	}

	if err := s.store.DB().Update(func(tx *bolt.Tx) error {
		_, err := s.setManifest(ctx, tx, req.GetManifest())
		if err != nil {
			return err
		}

		resp.Result = &emptypb.Empty{}

		return nil
	}); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *Writer) setManifest(ctx context.Context, tx *bolt.Tx, man *common.Manifest) (*common.Manifest, error) {
	resp := &common.Manifest{}

	mod, err := v3.Load(bytes.NewReader(man.GetBody()))
	if err != nil {
		return resp, err
	}

	r, err := mod.Reader()
	if err != nil {
		return resp, err
	}

	m := pb.NewStruct()
	if err := pb.BufToProto(r, m); err != nil {
		return resp, err
	}

	model := &common.Model{
		Model:     m,
		UpdatedAt: man.GetUpdatedAt(),
		Etag:      man.GetEtag(),
	}

	stats, err := ds.CalculateStats(ctx, tx)
	if err != nil {
		return resp, derr.ErrUnknown.Msgf("failed to calculate stats: %s", err.Error())
	}

	if err := s.store.MC().CanUpdate(mod, stats); err != nil {
		return resp, err
	}

	manifest, err := bdb.Set(ctx, tx, bdb.ManifestPathV2, bdb.ManifestKey, man)
	if err != nil {
		return resp, err
	}

	if _, err := bdb.Set(ctx, tx, bdb.ManifestPathV2, bdb.ModelKey, model); err != nil {
		return resp, err
	}

	if err := s.store.MC().UpdateModel(mod); err != nil {
		return resp, err
	}

	resp = manifest

	return resp, nil
}

func (s *Writer) DeleteManifest(ctx context.Context, req *writer.DeleteManifestRequest) (*writer.DeleteManifestResponse, error) {
	resp := &writer.DeleteManifestResponse{}

	if err := validator.DeleteManifestRequest(req); err != nil {
		return nil, err
	}

	if err := s.store.DB().Update(func(tx *bolt.Tx) error {
		err := s.deleteManifest(ctx, tx)
		if err != nil {
			return err
		}

		resp.Result = &emptypb.Empty{}

		return nil
	}); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *Writer) deleteManifest(ctx context.Context, tx *bolt.Tx) error {
	if err := bdb.Delete(ctx, tx, bdb.ManifestPathV2, bdb.ManifestKey); err != nil && status.Code(err) != codes.NotFound {
		return err
	}

	if err := bdb.Delete(ctx, tx, bdb.ManifestPathV2, bdb.ModelKey); err != nil && status.Code(err) != codes.NotFound {
		return err
	}

	if err := s.store.MC().UpdateModel(&model.Model{}); err != nil {
		return err
	}

	return nil
}
