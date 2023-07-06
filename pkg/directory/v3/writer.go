package v3

import (
	"context"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
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
		Object: &dsc2.Object{
			Type:        req.Object.Type,
			Key:         req.Object.Id,
			DisplayName: req.Object.DisplayName,
			Properties:  req.Object.Properties,
			CreatedAt:   req.Object.CreatedAt,
			UpdatedAt:   req.Object.UpdatedAt,
			Hash:        req.Object.Etag,
		},
	})

	if err != nil {
		return &dsw3.SetObjectResponse{}, err
	}

	return &dsw3.SetObjectResponse{
		Result: &dsc3.Object{
			Type:        resp.Result.Type,
			Id:          resp.Result.Key,
			DisplayName: resp.Result.DisplayName,
			Properties:  resp.Result.Properties,
			CreatedAt:   resp.Result.CreatedAt,
			UpdatedAt:   resp.Result.UpdatedAt,
			Etag:        resp.Result.Hash,
		},
	}, nil
}

func (s *Writer) DeleteObject(ctx context.Context, req *dsw3.DeleteObjectRequest) (*dsw3.DeleteObjectResponse, error) {
	resp, err := s.w2.DeleteObject(ctx, &dsw2.DeleteObjectRequest{
		Param: &dsc2.ObjectIdentifier{
			Type: &req.Object.Type,
			Key:  &req.Object.Id,
		},
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
		Relation: &dsc2.Relation{
			Object: &dsc2.ObjectIdentifier{
				Type: &req.Relation.ObjectType,
				Key:  &req.Relation.ObjectId,
			},
			Relation: req.Relation.Relation,
			Subject: &dsc2.ObjectIdentifier{
				Type: &req.Relation.SubjectType,
				Key:  &req.Relation.SubjectId,
			},
			CreatedAt: req.Relation.CreatedAt,
			UpdatedAt: req.Relation.UpdatedAt,
			Hash:      req.Relation.Etag,
		},
	})

	if err != nil {
		return &dsw3.SetRelationResponse{}, err
	}

	return &dsw3.SetRelationResponse{
		Result: &dsc3.Relation{
			ObjectType:  *resp.Result.Object.Type,
			ObjectId:    *resp.Result.Object.Key,
			Relation:    resp.Result.Relation,
			SubjectType: *resp.Result.Subject.Type,
			SubjectId:   *resp.Result.Subject.Key,
			CreatedAt:   resp.Result.CreatedAt,
			UpdatedAt:   resp.Result.UpdatedAt,
			Etag:        resp.Result.Hash,
		},
	}, nil
}

func (s *Writer) DeleteRelation(ctx context.Context, req *dsw3.DeleteRelationRequest) (*dsw3.DeleteRelationResponse, error) {
	resp, err := s.w2.DeleteRelation(ctx, &dsw2.DeleteRelationRequest{
		Param: &dsc2.RelationIdentifier{
			Object: &dsc2.ObjectIdentifier{
				Type: &req.Relation.ObjectType,
				Key:  &req.Relation.ObjectId,
			},
			Relation: &dsc2.RelationTypeIdentifier{
				ObjectType: &req.Relation.ObjectType,
				Name:       &req.Relation.Relation,
			},
			Subject: &dsc2.ObjectIdentifier{
				Type: &req.Relation.SubjectType,
				Key:  &req.Relation.SubjectId,
			},
		},
	})

	if err != nil {
		return &dsw3.DeleteRelationResponse{}, err
	}

	return &dsw3.DeleteRelationResponse{
		Result: resp.Result,
	}, nil
}
