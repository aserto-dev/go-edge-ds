package directory

import (
	"context"

	"github.com/aserto-dev/edge-ds/pkg/boltdb"
	"github.com/aserto-dev/edge-ds/pkg/types"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/v2"
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
	return nil, nil
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
	return nil, nil
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
	return nil, nil
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

	_, err = types.GetObjectMany(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	if err != nil {
		return nil, err
	}

	results := []*dsc.Object{}
	return &dsr.GetObjectManyResponse{Results: results}, err
}

func (s *Directory) GetObjects(ctx context.Context, req *dsr.GetObjectsRequest) (*dsr.GetObjectsResponse, error) {
	return nil, nil
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
	return nil, nil
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
