package directory

import (
	"context"

	"github.com/aserto-dev/edge-ds/pkg/boltdb"
	"github.com/aserto-dev/edge-ds/pkg/types"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/v2"
	"github.com/aserto-dev/go-grpc/aserto/api/v2"
)

// object type metadata methods
func (s *Directory) GetObjectType(ctx context.Context, req *dsr.GetObjectTypeRequest) (resp *dsr.GetObjectTypeResponse, err error) {
	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	objType, err := types.GetObjectType(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	return &dsr.GetObjectTypeResponse{Result: objType.Msg()}, err
}

func (s *Directory) GetObjectTypes(ctx context.Context, req *dsr.GetObjectTypesRequest) (*dsr.GetObjectTypesResponse, error) {
	if req.Page == nil {
		req.Page = &api.PaginationRequest{}
	}

	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	objTypes, page, err := types.GetObjectTypes(ctx, req.Page, s.store, []boltdb.Opts{txOpt}...)

	results := make([]*dsc.ObjectType, len(objTypes))
	for i := 0; i < len(objTypes); i++ {
		results[i] = objTypes[i].ObjectType
	}

	return &dsr.GetObjectTypesResponse{
		Results: results,
		Page:    page,
	}, err
}

// relation type metadata methods
func (s *Directory) GetRelationType(ctx context.Context, req *dsr.GetRelationTypeRequest) (*dsr.GetRelationTypeResponse, error) {
	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	relType, err := types.GetRelationType(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	return &dsr.GetRelationTypeResponse{Result: relType.Msg()}, err
}

func (s *Directory) GetRelationTypes(ctx context.Context, req *dsr.GetRelationTypesRequest) (*dsr.GetRelationTypesResponse, error) {
	if req.Page == nil {
		req.Page = &api.PaginationRequest{}
	}

	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	relTypes, page, err := types.GetRelationTypes(ctx, req.Page, s.store, []boltdb.Opts{txOpt}...)

	results := make([]*dsc.RelationType, len(relTypes))
	for i := 0; i < len(relTypes); i++ {
		results[i] = relTypes[i].RelationType
	}

	return &dsr.GetRelationTypesResponse{
		Results: results,
		Page:    page,
	}, err
}

// permission metadata methods
func (s *Directory) GetPermission(ctx context.Context, req *dsr.GetPermissionRequest) (*dsr.GetPermissionResponse, error) {
	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	perm, err := types.GetPermission(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	return &dsr.GetPermissionResponse{Result: perm.Msg()}, err
}

func (s *Directory) GetPermissions(ctx context.Context, req *dsr.GetPermissionsRequest) (*dsr.GetPermissionsResponse, error) {
	if req.Page == nil {
		req.Page = &api.PaginationRequest{}
	}

	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	permissions, page, err := types.GetPermissions(ctx, req.Page, s.store, []boltdb.Opts{txOpt}...)

	results := make([]*dsc.Permission, len(permissions))
	for i := 0; i < len(permissions); i++ {
		results[i] = permissions[i].Permission
	}

	return &dsr.GetPermissionsResponse{
		Results: results,
		Page:    page,
	}, err
}

// object methods
func (s *Directory) GetObject(ctx context.Context, req *dsr.GetObjectRequest) (*dsr.GetObjectResponse, error) {
	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	obj, err := types.GetObject(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	return &dsr.GetObjectResponse{Result: obj.Msg()}, err
}

func (s *Directory) GetObjectMany(ctx context.Context, req *dsr.GetObjectManyRequest) (*dsr.GetObjectManyResponse, error) {
	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	objects, err := types.GetObjectMany(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	if err != nil {
		return nil, err
	}

	results := make([]*dsc.Object, len(objects))
	for i := 0; i < len(objects); i++ {
		results[i] = objects[i].Object
	}
	return &dsr.GetObjectManyResponse{Results: results}, err
}

func (s *Directory) GetObjects(ctx context.Context, req *dsr.GetObjectsRequest) (*dsr.GetObjectsResponse, error) {
	if req.Page == nil {
		req.Page = &api.PaginationRequest{}
	}

	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	objects, page, err := types.GetObjects(ctx, req.Page, s.store, []boltdb.Opts{txOpt}...)

	results := make([]*dsc.Object, len(objects))
	for i := 0; i < len(objects); i++ {
		results[i] = objects[i].Object
	}

	return &dsr.GetObjectsResponse{
		Results: results,
		Page:    page,
	}, err
}

// relation methods
func (s *Directory) GetRelation(ctx context.Context, req *dsr.GetRelationRequest) (*dsr.GetRelationResponse, error) {
	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	rel, err := types.GetRelation(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	return &dsr.GetRelationResponse{Result: rel.Msg()}, err
}

func (s *Directory) GetRelations(ctx context.Context, req *dsr.GetRelationsRequest) (*dsr.GetRelationsResponse, error) {
	if req.Page == nil {
		req.Page = &api.PaginationRequest{}
	}

	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	relations, page, err := types.GetRelations(ctx, req.Page, s.store, []boltdb.Opts{txOpt}...)

	results := make([]*dsc.Relation, len(relations))
	for i := 0; i < len(relations); i++ {
		results[i] = relations[i].Relation
	}

	return &dsr.GetRelationsResponse{
		Results: results,
		Page:    page,
	}, err
}

// check methods
func (s *Directory) CheckPermission(ctx context.Context, req *dsr.CheckPermissionRequest) (*dsr.CheckPermissionResponse, error) {
	return nil, nil
}

func (s *Directory) CheckRelation(ctx context.Context, req *dsr.CheckRelationRequest) (*dsr.CheckRelationResponse, error) {
	return nil, nil
}

// graph methods
func (s *Directory) GetGraph(ctx context.Context, req *dsr.GetGraphRequest) (*dsr.GetGraphResponse, error) {
	return nil, nil
}
