package directory

import (
	"context"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
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
	objType, err := sc.GetObjectType(types.NewObjectTypeIdentifier(req.Param))
	return &dsr.GetObjectTypeResponse{Result: objType.Msg()}, err
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
	objTypes, page, err := sc.GetObjectTypes(types.NewPaginationRequest(req.Page))

	results := make([]*dsc.ObjectType, len(objTypes))
	for i := 0; i < len(objTypes); i++ {
		results[i] = objTypes[i].ObjectType
	}

	return &dsr.GetObjectTypesResponse{
		Results: results,
		Page:    page.PaginationResponse,
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
	relType, err := sc.GetRelationType(types.NewRelationTypeIdentifier(req.Param))
	return &dsr.GetRelationTypeResponse{Result: relType.Msg()}, err
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
	relTypes, page, err := sc.GetRelationTypes(types.NewObjectTypeIdentifier(req.Param), types.NewPaginationRequest(req.Page))

	results := make([]*dsc.RelationType, len(relTypes))
	for i := 0; i < len(relTypes); i++ {
		results[i] = relTypes[i].RelationType
	}

	return &dsr.GetRelationTypesResponse{
		Results: results,
		Page:    page.PaginationResponse,
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
	perm, err := sc.GetPermission(types.NewPermissionIdentifier(req.Param))
	return &dsr.GetPermissionResponse{Result: perm.Msg()}, err
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
	permissions, page, err := sc.GetPermissions(types.NewPaginationRequest(req.Page))

	results := make([]*dsc.Permission, len(permissions))
	for i := 0; i < len(permissions); i++ {
		results[i] = permissions[i].Permission
	}

	return &dsr.GetPermissionsResponse{
		Results: results,
		Page:    page.PaginationResponse,
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
	obj, err := sc.GetObject(types.NewObjectIdentifier(req.Param))
	return &dsr.GetObjectResponse{Result: obj.Msg()}, err
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
	objIdentifiers := []*types.ObjectIdentifier{}
	for i := 0; i < len(req.Param); i++ {
		objIdentifiers = append(objIdentifiers, &types.ObjectIdentifier{ObjectIdentifier: req.Param[i]})
	}
	objects, err := sc.GetObjectMany(objIdentifiers)
	if err != nil {
		return nil, err
	}

	results := make([]*dsc.Object, len(objects))
	for i := 0; i < len(objects); i++ {
		results[i] = objects[i].Object
	}
	return &dsr.GetObjectManyResponse{Results: results}, err
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
	objects, page, err := sc.GetObjects(types.NewObjectTypeIdentifier(req.Param), types.NewPaginationRequest(req.Page))

	results := make([]*dsc.Object, len(objects))
	for i := 0; i < len(objects); i++ {
		results[i] = objects[i].Object
	}

	return &dsr.GetObjectsResponse{
		Results: results,
		Page:    page.PaginationResponse,
	}, err
}

// Get relation instances based on subject, relation, object filter.
func (s *Directory) GetRelation(ctx context.Context, req *dsr.GetRelationRequest) (resp *dsr.GetRelationResponse, err error) {
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
	relations, err := sc.GetRelation(types.NewRelationIdentifier(req.Param))

	results := make([]*dsc.Relation, len(relations))
	objects := map[string]*dsc.Object{}

	for i := 0; i < len(relations); i++ {
		results[i] = relations[i].Relation

		if req.GetWithObjects() {
			sub, err := sc.GetObject(types.NewObjectIdentifier(results[i].Subject))
			if err != nil {
				return &dsr.GetRelationResponse{}, err
			}

			obj, err := sc.GetObject(types.NewObjectIdentifier(results[i].Object))
			if err != nil {
				return &dsr.GetRelationResponse{}, err
			}

			// TODO: validate
			objects[sub.String()] = sub.Msg()
			objects[obj.String()] = obj.Msg()
		}
	}

	return &dsr.GetRelationResponse{
		Results: results,
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
	relations, page, err := sc.GetRelations(types.NewRelationIdentifier(req.Param), types.NewPaginationRequest(req.Page))

	results := make([]*dsc.Relation, len(relations))
	for i := 0; i < len(relations); i++ {
		results[i] = relations[i].Relation
	}

	return &dsr.GetRelationsResponse{
		Results: results,
		Page:    page.PaginationResponse,
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

	results := make([]*dsc.ObjectDependency, len(dependencies))
	for i := 0; i < len(dependencies); i++ {
		results[i] = dependencies[i].ObjectDependency
	}

	return &dsr.GetGraphResponse{
		Results: results,
	}, nil
}
