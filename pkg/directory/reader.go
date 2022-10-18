package directory

import (
	"context"

	"github.com/aserto-dev/edge-ds/pkg/boltdb"
	"github.com/aserto-dev/edge-ds/pkg/types"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
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
		req.Page = &dsc.PaginationRequest{}
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
		req.Page = &dsc.PaginationRequest{}
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

	relTypes, page, err := types.GetRelationTypes(ctx, req, s.store, []boltdb.Opts{txOpt}...)

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
		req.Page = &dsc.PaginationRequest{}
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
		req.Page = &dsc.PaginationRequest{}
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

	var relations []*types.Relation
	var resp *dsc.PaginationResponse
	var results []*dsc.Relation
	hasNext := true
	nextToken := ""

	for hasNext {
		relations, resp, err = types.GetRelations(ctx, &dsr.GetRelationsRequest{
			Param: req.Param,
			Page:  &dsc.PaginationRequest{Token: nextToken},
		}, s.store, []boltdb.Opts{txOpt}...)

		for _, rel := range relations {
			results = append(results, rel.Relation)
		}

		nextToken = resp.NextToken
		if nextToken == "" {
			hasNext = false
		}
	}

	return &dsr.GetRelationResponse{
		Results: results,
	}, nil
}

func (s *Directory) GetRelations(ctx context.Context, req *dsr.GetRelationsRequest) (*dsr.GetRelationsResponse, error) {
	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{}
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

	relations, page, err := types.GetRelations(ctx, req, s.store, []boltdb.Opts{txOpt}...)

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

	result, err := types.CheckPermission(ctx, req, s.store, []boltdb.Opts{txOpt}...)

	return &dsr.CheckPermissionResponse{
		Check: result.Check,
		Trace: result.Trace,
	}, err
}

func (s *Directory) CheckRelation(ctx context.Context, req *dsr.CheckRelationRequest) (*dsr.CheckRelationResponse, error) {
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

	result, err := types.CheckRelation(ctx, req, s.store, []boltdb.Opts{txOpt}...)

	return &dsr.CheckRelationResponse{
		Check: result.Check,
		Trace: result.Trace,
	}, err
}

// graph methods
func (s *Directory) GetGraph(ctx context.Context, req *dsr.GetGraphRequest) (*dsr.GetGraphResponse, error) {
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

	dependencies, err := types.GetGraph(ctx, req, s.store, []boltdb.Opts{txOpt}...)

	results := make([]*dsc.ObjectDependency, len(dependencies))
	for i := 0; i < len(dependencies); i++ {
		results[i] = dependencies[i].ObjectDependency
	}

	return &dsr.GetGraphResponse{
		Results: results,
	}, nil
}
