package v3

import (
	"context"

	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/convert"
	v2 "github.com/aserto-dev/go-edge-ds/pkg/directory/v2"

	"github.com/rs/zerolog"
)

type Writer struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	w2     dsw2.WriterServer
}

func NewWriter(logger *zerolog.Logger, store *bdb.BoltDB, w *v2.Writer) *Writer {
	return &Writer{
		logger: logger,
		store:  store,
		w2:     w,
	}
}

// object methods.
func (s *Writer) SetObject(ctx context.Context, req *dsw3.SetObjectRequest) (*dsw3.SetObjectResponse, error) {
	resp, err := s.w2.SetObject(ctx, &dsw2.SetObjectRequest{
		Object: convert.Object3(req.Object),
	})

	if err != nil {
		return &dsw3.SetObjectResponse{}, err
	}

	return &dsw3.SetObjectResponse{
		Result: convert.Object2(resp.Result),
	}, nil
}

func (s *Writer) DeleteObject(ctx context.Context, req *dsw3.DeleteObjectRequest) (*dsw3.DeleteObjectResponse, error) {
	resp, err := s.w2.DeleteObject(ctx, &dsw2.DeleteObjectRequest{
		Param: convert.ObjectIdentifier3(req.Object),
	})

	if err != nil {
		return &dsw3.DeleteObjectResponse{}, err
	}

	return &dsw3.DeleteObjectResponse{
		Result: resp.Result,
	}, nil
}

// relation methods.
func (s *Writer) SetRelation(ctx context.Context, req *dsw3.SetRelationRequest) (*dsw3.SetRelationResponse, error) {
	resp, err := s.w2.SetRelation(ctx, &dsw2.SetRelationRequest{
		Relation: convert.Relation3(req.Relation),
	})

	if err != nil {
		return &dsw3.SetRelationResponse{}, err
	}

	return &dsw3.SetRelationResponse{
		Result: convert.Relation2(resp.Result),
	}, nil
}

func (s *Writer) DeleteRelation(ctx context.Context, req *dsw3.DeleteRelationRequest) (*dsw3.DeleteRelationResponse, error) {
	resp, err := s.w2.DeleteRelation(ctx, &dsw2.DeleteRelationRequest{
		Param: convert.RelationIdentifier3(req.Relation),
	})

	if err != nil {
		return &dsw3.DeleteRelationResponse{}, err
	}

	return &dsw3.DeleteRelationResponse{
		Result: resp.Result,
	}, nil
}
