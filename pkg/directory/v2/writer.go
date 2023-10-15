package v2

import (
	"context"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	v3 "github.com/aserto-dev/go-edge-ds/pkg/directory/v3"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	errMetaDataMethodObsolete string = "method %s is obsolete, use set manifest to manipulate metadata"
)

type Writer struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	w3     dsw3.WriterServer
}

func NewWriter(logger *zerolog.Logger, store *bdb.BoltDB, w3 *v3.Writer) *Writer {
	return &Writer{
		logger: logger,
		store:  store,
		w3:     w3,
	}
}

// SetObjectType, obsolete, use set manifest to manipulate metadata.
func (s *Writer) SetObjectType(_ context.Context, _ *dsw2.SetObjectTypeRequest) (*dsw2.SetObjectTypeResponse, error) {
	return &dsw2.SetObjectTypeResponse{}, status.Errorf(codes.Unimplemented, errMetaDataMethodObsolete, "SetObjectType")
}

// DeleteObjectType, obsolete, use set manifest to manipulate metadata.
func (s *Writer) DeleteObjectType(_ context.Context, _ *dsw2.DeleteObjectTypeRequest) (*dsw2.DeleteObjectTypeResponse, error) {
	return &dsw2.DeleteObjectTypeResponse{}, status.Errorf(codes.Unimplemented, errMetaDataMethodObsolete, "DeleteObjectType")
}

// SetRelationType, obsolete, use set manifest to manipulate metadata.
func (s *Writer) SetRelationType(_ context.Context, _ *dsw2.SetRelationTypeRequest) (*dsw2.SetRelationTypeResponse, error) {
	return &dsw2.SetRelationTypeResponse{}, status.Errorf(codes.Unimplemented, errMetaDataMethodObsolete, "SetRelationType")
}

// DeleteRelationType, obsolete, use set manifest to manipulate metadata.
func (s *Writer) DeleteRelationType(_ context.Context, _ *dsw2.DeleteRelationTypeRequest) (*dsw2.DeleteRelationTypeResponse, error) {
	return &dsw2.DeleteRelationTypeResponse{}, status.Errorf(codes.Unimplemented, errMetaDataMethodObsolete, "DeleteRelationType")
}

// SetPermission, obsolete, use set manifest to manipulate metadata.
func (s *Writer) SetPermission(_ context.Context, _ *dsw2.SetPermissionRequest) (*dsw2.SetPermissionResponse, error) {
	return &dsw2.SetPermissionResponse{}, status.Errorf(codes.Unimplemented, errMetaDataMethodObsolete, "SetPermission")
}

// DeletePermission, obsolete, use set manifest to manipulate metadata.
func (s *Writer) DeletePermission(_ context.Context, _ *dsw2.DeletePermissionRequest) (*dsw2.DeletePermissionResponse, error) {
	return &dsw2.DeletePermissionResponse{}, status.Errorf(codes.Unimplemented, errMetaDataMethodObsolete, "DeletePermission")
}

// SetObject, implementation is delegated to writer.v3.SetObject.
func (s *Writer) SetObject(ctx context.Context, req *dsw2.SetObjectRequest) (*dsw2.SetObjectResponse, error) {
	r3, err := s.w3.SetObject(ctx, &dsw3.SetObjectRequest{
		Object: &dsc3.Object{
			Type:        req.GetObject().GetType(),
			Id:          req.GetObject().GetKey(),
			DisplayName: req.GetObject().GetDisplayName(),
			Properties:  req.GetObject().GetProperties(),
			CreatedAt:   req.GetObject().GetCreatedAt(),
			UpdatedAt:   req.GetObject().GetUpdatedAt(),
			Etag:        req.GetObject().GetHash(),
		},
	})
	if err != nil {
		return &dsw2.SetObjectResponse{}, err
	}

	r2 := &dsw2.SetObjectResponse{
		Result: &dsc2.Object{
			Type:        r3.GetResult().GetType(),
			Key:         r3.GetResult().GetId(),
			DisplayName: r3.GetResult().GetDisplayName(),
			Properties:  r3.GetResult().GetProperties(),
			CreatedAt:   r3.GetResult().GetCreatedAt(),
			UpdatedAt:   r3.GetResult().GetUpdatedAt(),
			Hash:        r3.GetResult().GetEtag(),
		},
	}

	return r2, err
}

// DeleteObject, implementation is delegated to writer.v3.DeleteObject.
func (s *Writer) DeleteObject(ctx context.Context, req *dsw2.DeleteObjectRequest) (*dsw2.DeleteObjectResponse, error) {
	r3, err := s.w3.DeleteObject(ctx, &dsw3.DeleteObjectRequest{
		ObjectType:    req.GetParam().GetType(),
		ObjectId:      req.GetParam().GetKey(),
		WithRelations: proto.Bool(req.GetWithRelations()),
	})
	if err != nil {
		return &dsw2.DeleteObjectResponse{}, err
	}

	r2 := &dsw2.DeleteObjectResponse{
		Result: r3.Result,
	}

	return r2, err
}

// SetRelation, implementation is delegated to writer.v3.SetRelation.
func (s *Writer) SetRelation(ctx context.Context, req *dsw2.SetRelationRequest) (*dsw2.SetRelationResponse, error) {
	r3, err := s.w3.SetRelation(ctx, &dsw3.SetRelationRequest{
		Relation: &dsc3.Relation{
			ObjectType:      req.GetRelation().GetObject().GetType(),
			ObjectId:        req.GetRelation().GetObject().GetKey(),
			Relation:        req.GetRelation().GetRelation(),
			SubjectType:     req.GetRelation().GetSubject().GetType(),
			SubjectId:       req.GetRelation().GetSubject().GetKey(),
			SubjectRelation: "",
		},
	})
	if err != nil {
		return &dsw2.SetRelationResponse{}, err
	}

	r2 := &dsw2.SetRelationResponse{
		Result: &dsc2.Relation{
			Object: &dsc2.ObjectIdentifier{
				Type: proto.String(r3.GetResult().GetObjectType()),
				Key:  proto.String(r3.GetResult().GetObjectId()),
			},
			Relation: r3.GetResult().GetRelation(),
			Subject: &dsc2.ObjectIdentifier{
				Type: proto.String(r3.GetResult().GetSubjectType()),
				Key:  proto.String(r3.GetResult().GetSubjectId()),
			},
			CreatedAt: r3.GetResult().GetCreatedAt(),
			UpdatedAt: r3.GetResult().GetUpdatedAt(),
			Hash:      r3.GetResult().GetEtag(),
		},
	}

	return r2, err
}

// DeleteRelation, implementation is delegated to writer.v3.DeleteRelation.
func (s *Writer) DeleteRelation(ctx context.Context, req *dsw2.DeleteRelationRequest) (*dsw2.DeleteRelationResponse, error) {
	r3, err := s.w3.DeleteRelation(ctx, &dsw3.DeleteRelationRequest{
		ObjectType:      req.GetParam().GetObject().GetType(),
		ObjectId:        req.GetParam().GetObject().GetKey(),
		Relation:        req.GetParam().GetRelation().GetName(),
		SubjectType:     req.GetParam().GetSubject().GetType(),
		SubjectId:       req.GetParam().GetSubject().GetKey(),
		SubjectRelation: "",
	})
	if err != nil {
		return &dsw2.DeleteRelationResponse{}, err
	}

	r2 := &dsw2.DeleteRelationResponse{
		Result: r3.Result,
	}

	return r2, err
}
