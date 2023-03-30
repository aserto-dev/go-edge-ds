package directory

import (
	"context"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/aserto-dev/go-edge-ds/pkg/types"
)

// Get object type (metadata).
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	objType, err := sc.GetObjectType(req.Param)
	return &dsr.GetObjectTypeResponse{Result: objType}, err
}

// Get all objects types (metadata) (paginated).
func (s *Directory) GetObjectTypes(ctx context.Context, req *dsr.GetObjectTypesRequest) (resp *dsr.GetObjectTypesResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	objTypes, page, err := sc.GetObjectTypes(req.Page)

	return &dsr.GetObjectTypesResponse{
		Results: objTypes,
		Page:    page,
	}, err
}

// Get relation type (metadata).
func (s *Directory) GetRelationType(ctx context.Context, req *dsr.GetRelationTypeRequest) (resp *dsr.GetRelationTypeResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	relType, err := sc.GetRelationType(req.Param)
	return &dsr.GetRelationTypeResponse{Result: relType}, err
}

// Get all relation types, optionally filtered by object type (metadata) (paginated).
func (s *Directory) GetRelationTypes(ctx context.Context, req *dsr.GetRelationTypesRequest) (resp *dsr.GetRelationTypesResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	relTypes, page, err := sc.GetRelationTypes(req.Param, req.Page)

	return &dsr.GetRelationTypesResponse{
		Results: relTypes,
		Page:    page,
	}, err
}

// Get permission (metadata).
func (s *Directory) GetPermission(ctx context.Context, req *dsr.GetPermissionRequest) (resp *dsr.GetPermissionResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	perm, err := sc.GetPermission(req.Param)
	return &dsr.GetPermissionResponse{Result: perm}, err
}

// Get all permissions (metadata) (paginated).
func (s *Directory) GetPermissions(ctx context.Context, req *dsr.GetPermissionsRequest) (resp *dsr.GetPermissionsResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	permissions, page, err := sc.GetPermissions(req.Page)

	return &dsr.GetPermissionsResponse{
		Results: permissions,
		Page:    page,
	}, err
}

// Get single object instance.
func (s *Directory) GetObject(ctx context.Context, req *dsr.GetObjectRequest) (resp *dsr.GetObjectResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	obj, err := sc.GetObject(req.Param)
	return &dsr.GetObjectResponse{Result: obj}, err
}

// Get multiple object instances by id or type+key, in a single request.
func (s *Directory) GetObjectMany(ctx context.Context, req *dsr.GetObjectManyRequest) (resp *dsr.GetObjectManyResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	objects, err := sc.GetObjectMany(req.Param)
	if err != nil {
		return nil, err
	}

	return &dsr.GetObjectManyResponse{
		Results: objects,
	}, err
}

// Get all object instances, optionally filtered by object type. (paginated).
func (s *Directory) GetObjects(ctx context.Context, req *dsr.GetObjectsRequest) (resp *dsr.GetObjectsResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	objects, page, err := sc.GetObjects(req.Param, req.Page)

	return &dsr.GetObjectsResponse{
		Results: objects,
		Page:    page,
	}, err
}

// Get relation instances based on subject, relation, object filter.
func (s *Directory) GetRelation(ctx context.Context, req *dsr.GetRelationRequest) (resp *dsr.GetRelationResponse, err error) {
	if ok, err := ds.RelationIdentifier(req.Param).Validate(); !ok {
		return &dsr.GetRelationResponse{}, err
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	relations, err := sc.GetRelation(req.Param)

	objects := map[string]*dsc.Object{}

	if req.GetWithObjects() {
		for i := 0; i < len(relations); i++ {
			sub, err := sc.GetObject(relations[i].Subject)
			if err != nil {
				return &dsr.GetRelationResponse{}, err
			}

			obj, err := sc.GetObject(relations[i].Object)
			if err != nil {
				return &dsr.GetRelationResponse{}, err
			}

			objects[sub.String()] = sub
			objects[obj.String()] = obj
		}
	}

	return &dsr.GetRelationResponse{
		Results: relations,
		Objects: objects,
	}, nil
}

// Get relation instances based on subject, relation, object filter (paginated).
func (s *Directory) GetRelations(ctx context.Context, req *dsr.GetRelationsRequest) (resp *dsr.GetRelationsResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	relations, page, err := sc.GetRelations(req.Param, req.Page)

	return &dsr.GetRelationsResponse{
		Results: relations,
		Page:    page,
	}, err
}

// Check if subject has permission on object.
func (s *Directory) CheckPermission(ctx context.Context, req *dsr.CheckPermissionRequest) (resp *dsr.CheckPermissionResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	result, err := sc.CheckPermission(req)

	return &dsr.CheckPermissionResponse{
		Check: result.Check,
		Trace: result.Trace,
	}, err
}

// Check if subject has relation to object.
func (s *Directory) CheckRelation(ctx context.Context, req *dsr.CheckRelationRequest) (resp *dsr.CheckRelationResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	result, err := sc.CheckRelation(req)
	if err != nil {
		return &dsr.CheckRelationResponse{}, err
	}

	return &dsr.CheckRelationResponse{
		Check: result.Check,
		Trace: result.Trace,
	}, err
}

// Get object dependency graph.
func (s *Directory) GetGraph(ctx context.Context, req *dsr.GetGraphRequest) (resp *dsr.GetGraphResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	dependencies, err := sc.GetGraph(req)

	return &dsr.GetGraphResponse{
		Results: dependencies,
	}, nil
}
