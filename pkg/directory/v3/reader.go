package v3

import (
	"context"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/convert"
	v2 "github.com/aserto-dev/go-edge-ds/pkg/directory/v2"

	"github.com/rs/zerolog"
)

type Reader struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	r2     dsr2.ReaderServer
}

func NewReader(logger *zerolog.Logger, store *bdb.BoltDB, r *v2.Reader) *Reader {
	return &Reader{
		logger: logger,
		store:  store,
		r2:     r,
	}
}

// object type metadata methods.
func (s *Reader) GetObjectType(context.Context, *dsr3.GetObjectTypeRequest) (*dsr3.GetObjectTypeResponse, error) {
	return &dsr3.GetObjectTypeResponse{}, nil
}

func (s *Reader) GetObjectTypes(context.Context, *dsr3.GetObjectTypesRequest) (*dsr3.GetObjectTypesResponse, error) {
	return &dsr3.GetObjectTypesResponse{}, nil
}

// relation type metadata methods.
func (s *Reader) GetRelationType(context.Context, *dsr3.GetRelationTypeRequest) (*dsr3.GetRelationTypeResponse, error) {
	return &dsr3.GetRelationTypeResponse{}, nil
}

func (s *Reader) GetRelationTypes(context.Context, *dsr3.GetRelationTypesRequest) (*dsr3.GetRelationTypesResponse, error) {
	return &dsr3.GetRelationTypesResponse{}, nil
}

// permission metadata methods.
func (s *Reader) GetPermission(context.Context, *dsr3.GetPermissionRequest) (*dsr3.GetPermissionResponse, error) {
	return &dsr3.GetPermissionResponse{}, nil
}

func (s *Reader) GetPermissions(context.Context, *dsr3.GetPermissionsRequest) (*dsr3.GetPermissionsResponse, error) {
	return &dsr3.GetPermissionsResponse{}, nil
}

// object methods.
func (s *Reader) GetObject(ctx context.Context, req *dsr3.GetObjectRequest) (*dsr3.GetObjectResponse, error) {
	resp, err := s.r2.GetObject(ctx, &dsr2.GetObjectRequest{
		Param: &dsc2.ObjectIdentifier{
			Type: &req.ObjectType,
			Key:  &req.ObjectId,
		},
		WithRelations: req.WithRelations,
	})

	if err != nil {
		return &dsr3.GetObjectResponse{}, err
	}

	incoming := make([]*dsc3.Relation, len(resp.Incoming))
	for i, r := range resp.Incoming {
		incoming[i] = convert.Relation2(r)
	}

	outgoing := make([]*dsc3.Relation, len(resp.Outgoing))
	for i, r := range resp.Outgoing {
		outgoing[i] = convert.Relation2(r)
	}

	return &dsr3.GetObjectResponse{
		Result:   convert.Object2(resp.Result),
		Incoming: incoming,
		Outgoing: outgoing,
	}, nil
}

func (s *Reader) GetObjectMany(ctx context.Context, req *dsr3.GetObjectManyRequest) (*dsr3.GetObjectManyResponse, error) {
	param := make([]*dsc2.ObjectIdentifier, len(req.Param))
	for i, p := range req.Param {
		param[i] = &dsc2.ObjectIdentifier{
			Type: &p.ObjectType,
			Key:  &p.ObjectId,
		}
	}

	resp, err := s.r2.GetObjectMany(ctx, &dsr2.GetObjectManyRequest{
		Param: param,
	})

	if err != nil {
		return &dsr3.GetObjectManyResponse{}, err
	}

	results := make([]*dsc3.Object, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = convert.Object2(r)
	}

	return &dsr3.GetObjectManyResponse{
		Results: results,
	}, nil
}

func (s *Reader) GetObjects(ctx context.Context, req *dsr3.GetObjectsRequest) (*dsr3.GetObjectsResponse, error) {
	resp, err := s.r2.GetObjects(ctx, &dsr2.GetObjectsRequest{
		Param: &dsc2.ObjectTypeIdentifier{
			Name: &req.ObjectType,
		},
		Page: convert.PaginationRequest3(req.Page),
	})

	if err != nil {
		return &dsr3.GetObjectsResponse{}, err
	}

	results := make([]*dsc3.Object, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = convert.Object2(r)
	}

	return &dsr3.GetObjectsResponse{
		Results: results,
		Page:    convert.PaginationResponse2(resp.Page),
	}, nil
}

// relation methods.
func (s *Reader) GetRelation(ctx context.Context, req *dsr3.GetRelationRequest) (*dsr3.GetRelationResponse, error) {
	resp, err := s.r2.GetRelation(ctx, &dsr2.GetRelationRequest{
		Param: &dsc2.RelationIdentifier{
			Object: &dsc2.ObjectIdentifier{
				Type: &req.ObjectType,
				Key:  &req.ObjectId,
			},
			Relation: &dsc2.RelationTypeIdentifier{
				ObjectType: &req.ObjectType,
				Name:       &req.Relation,
			},
			Subject: &dsc2.ObjectIdentifier{
				Type: &req.SubjectType,
				Key:  &req.SubjectId,
			},
		},
		WithObjects: req.WithObjects,
	})

	if err != nil {
		return &dsr3.GetRelationResponse{}, err
	}

	results := make([]*dsc3.Relation, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = convert.Relation2(r)
	}

	objects := make(map[string]*dsc3.Object, len(resp.Objects))
	for k, v := range resp.Objects {
		objects[k] = convert.Object2(v)
	}

	return &dsr3.GetRelationResponse{
		Results: results,
		Objects: objects,
	}, nil
}

func (s *Reader) GetRelations(ctx context.Context, req *dsr3.GetRelationsRequest) (*dsr3.GetRelationsResponse, error) {
	resp, err := s.r2.GetRelations(ctx, &dsr2.GetRelationsRequest{
		Param: &dsc2.RelationIdentifier{
			Object: &dsc2.ObjectIdentifier{
				Type: &req.ObjectType,
				Key:  &req.ObjectId,
			},
			Relation: &dsc2.RelationTypeIdentifier{
				ObjectType: &req.ObjectType,
				Name:       &req.Relation,
			},
			Subject: &dsc2.ObjectIdentifier{
				Type: &req.SubjectType,
				Key:  &req.SubjectId,
			},
		},
		Page: convert.PaginationRequest3(req.Page),
	})

	if err != nil {
		return &dsr3.GetRelationsResponse{}, err
	}

	results := make([]*dsc3.Relation, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = convert.Relation2(r)
	}

	return &dsr3.GetRelationsResponse{
		Results: results,
		Page:    convert.PaginationResponse2(resp.Page),
	}, nil
}

// check permission method.
func (s *Reader) CheckPermission(ctx context.Context, req *dsr3.CheckPermissionRequest) (*dsr3.CheckPermissionResponse, error) {
	resp, err := s.r2.CheckPermission(ctx, &dsr2.CheckPermissionRequest{
		Object: &dsc2.ObjectIdentifier{
			Type: &req.ObjectType,
			Key:  &req.ObjectId,
		},
		Permission: &dsc2.PermissionIdentifier{
			Name: &req.Permission,
		},
		Subject: &dsc2.ObjectIdentifier{
			Type: &req.SubjectType,
			Key:  &req.SubjectId,
		},
	})

	if err != nil {
		return &dsr3.CheckPermissionResponse{}, err
	}

	return &dsr3.CheckPermissionResponse{
		Check: resp.Check,
		Trace: resp.Trace,
	}, nil
}

// check relation method.
func (s *Reader) CheckRelation(ctx context.Context, req *dsr3.CheckRelationRequest) (*dsr3.CheckRelationResponse, error) {
	resp, err := s.r2.CheckRelation(ctx, &dsr2.CheckRelationRequest{
		Object: &dsc2.ObjectIdentifier{
			Type: &req.ObjectType,
			Key:  &req.ObjectId,
		},
		Relation: &dsc2.RelationTypeIdentifier{
			ObjectType: &req.ObjectType,
			Name:       &req.Relation,
		},
		Subject: &dsc2.ObjectIdentifier{
			Type: &req.SubjectType,
			Key:  &req.SubjectId,
		},
	})

	if err != nil {
		return &dsr3.CheckRelationResponse{}, err
	}

	return &dsr3.CheckRelationResponse{
		Check: resp.Check,
		Trace: resp.Trace,
	}, nil
}

// graph methods.
func (s *Reader) GetGraph(ctx context.Context, req *dsr3.GetGraphRequest) (*dsr3.GetGraphResponse, error) {
	resp, err := s.r2.GetGraph(ctx, &dsr2.GetGraphRequest{
		Anchor: &dsc2.ObjectIdentifier{
			Type: &req.AnchorType,
			Key:  &req.AnchorId,
		},
		Object: &dsc2.ObjectIdentifier{
			Type: &req.ObjectType,
			Key:  &req.ObjectId,
		},
		Relation: &dsc2.RelationTypeIdentifier{
			ObjectType: &req.ObjectType,
			Name:       &req.Relation,
		},
		Subject: &dsc2.ObjectIdentifier{
			Type: &req.SubjectType,
			Key:  &req.SubjectId,
		},
	})

	if err != nil {
		return &dsr3.GetGraphResponse{}, err
	}

	results := make([]*dsc3.ObjectDependency, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = convert.ObjectDependency2(r)
	}

	return &dsr3.GetGraphResponse{
		Results: results,
	}, nil
}
