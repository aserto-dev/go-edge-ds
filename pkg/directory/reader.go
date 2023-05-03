package directory

import (
	"context"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	bolt "go.etcd.io/bbolt"
)

// Get object type (metadata).
func (s *Directory) GetObjectType(ctx context.Context, req *dsr.GetObjectTypeRequest) (*dsr.GetObjectTypeResponse, error) {
	resp := &dsr.GetObjectTypeResponse{}

	if ok, err := ds.ObjectTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		objType, err := ds.Get[dsc.ObjectType](ctx, tx, ds.ObjectTypesPath, ds.ObjectTypeIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

// Get all objects types (metadata) (paginated).
func (s *Directory) GetObjectTypes(ctx context.Context, req *dsr.GetObjectTypesRequest) (*dsr.GetObjectTypesResponse, error) {
	resp := &dsr.GetObjectTypesResponse{Results: []*dsc.ObjectType{}}

	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{Size: 100}
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		results, page, err := ds.List[dsc.ObjectType](ctx, tx, ds.ObjectTypesPath, req.Page)
		if err != nil {
			return err
		}

		resp.Results = results
		resp.Page = page

		return nil
	})

	return resp, err
}

// Get relation type (metadata).
func (s *Directory) GetRelationType(ctx context.Context, req *dsr.GetRelationTypeRequest) (*dsr.GetRelationTypeResponse, error) {
	resp := &dsr.GetRelationTypeResponse{}

	if ok, err := ds.RelationTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		relType, err := ds.Get[dsc.RelationType](ctx, tx, ds.RelationTypesPath, ds.RelationTypeIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = relType
		return nil
	})

	return resp, err
}

// Get all relation types, optionally filtered by object type (metadata) (paginated).
func (s *Directory) GetRelationTypes(ctx context.Context, req *dsr.GetRelationTypesRequest) (*dsr.GetRelationTypesResponse, error) {
	resp := &dsr.GetRelationTypesResponse{Results: []*dsc.RelationType{}, Page: &dsc.PaginationResponse{}}

	if req.Param == nil {
		req.Param = &dsc.ObjectTypeIdentifier{}
	}

	if ok, err := ds.ObjectTypeSelector(req.Param).Validate(); !ok {
		return resp, err
	}

	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{Size: 100}
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		results, page, err := ds.List[dsc.RelationType](ctx, tx, ds.RelationTypesPath, req.Page)
		if err != nil {
			return err
		}

		resp.Results = results
		resp.Page = page

		return nil
	})

	return resp, err
}

// Get permission (metadata).
func (s *Directory) GetPermission(ctx context.Context, req *dsr.GetPermissionRequest) (*dsr.GetPermissionResponse, error) {
	resp := &dsr.GetPermissionResponse{}

	if ok, err := ds.PermissionIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		objType, err := ds.Get[dsc.Permission](ctx, tx, ds.PermissionsPath, ds.PermissionIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

// Get all permissions (metadata) (paginated).
func (s *Directory) GetPermissions(ctx context.Context, req *dsr.GetPermissionsRequest) (*dsr.GetPermissionsResponse, error) {
	resp := &dsr.GetPermissionsResponse{Results: []*dsc.Permission{}}

	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{Size: 100}
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		results, page, err := ds.List[dsc.Permission](ctx, tx, ds.PermissionsPath, req.Page)
		if err != nil {
			return err
		}

		resp.Results = results
		resp.Page = page

		return nil
	})

	return resp, err
}

// Get single object instance.
func (s *Directory) GetObject(ctx context.Context, req *dsr.GetObjectRequest) (*dsr.GetObjectResponse, error) {
	resp := &dsr.GetObjectResponse{}

	if ok, err := ds.ObjectIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		obj, err := ds.Get[dsc.Object](ctx, tx, ds.ObjectsPath, ds.ObjectIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = obj
		return nil
	})

	return resp, err
}

// Get multiple object instances by id or type+key, in a single request.
func (s *Directory) GetObjectMany(ctx context.Context, req *dsr.GetObjectManyRequest) (*dsr.GetObjectManyResponse, error) {
	resp := &dsr.GetObjectManyResponse{Results: []*dsc.Object{}}

	if req.Param == nil {
		req.Param = []*dsc.ObjectIdentifier{}
	}

	// validate all object identifiers first.
	for _, i := range req.Param {
		if ok, err := ds.ObjectIdentifier(i).Validate(); !ok {
			return resp, err
		}
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		for _, i := range req.Param {
			obj, err := ds.Get[dsc.Object](ctx, tx, ds.ObjectsPath, ds.ObjectIdentifier(i).Key())
			if err != nil {
				return err
			}
			resp.Results = append(resp.Results, obj)
		}
		return nil
	})

	return resp, err
}

// Get all object instances, optionally filtered by object type. (paginated).
func (s *Directory) GetObjects(ctx context.Context, req *dsr.GetObjectsRequest) (*dsr.GetObjectsResponse, error) {
	resp := &dsr.GetObjectsResponse{Results: []*dsc.Object{}, Page: &dsc.PaginationResponse{}}

	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{Size: 100}
	}

	if ok, err := ds.ObjectTypeSelector(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		results, page, err := ds.List[dsc.Object](ctx, tx, ds.ObjectsPath, req.Page)
		if err != nil {
			return err
		}

		resp.Results = results
		resp.Page = page

		return nil
	})

	return resp, err
}

// Get relation instances based on subject, relation, object filter.
func (s *Directory) GetRelation(ctx context.Context, req *dsr.GetRelationRequest) (*dsr.GetRelationResponse, error) {
	resp := &dsr.GetRelationResponse{Results: []*dsc.Relation{}, Objects: map[string]*dsc.Object{}}

	if ok, err := ds.RelationIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		rel, err := ds.Get[dsc.Relation](ctx, tx, ds.RelationsObjPath, ds.RelationIdentifier(req.Param).ObjKey())
		if err != nil {
			return err
		}

		resp.Results = append(resp.Results, rel)

		if req.GetWithObjects() {
			objects := map[string]*dsc.Object{}
			for i := 0; i < len(resp.Results); i++ {
				sub, err := ds.Get[dsc.Object](ctx, tx, ds.ObjectsPath, ds.ObjectIdentifier(rel.Subject).Key())
				if err != nil {
					return err
				}
				objects[ds.ObjectIdentifier(rel.Subject).Key()] = sub

				obj, err := ds.Get[dsc.Object](ctx, tx, ds.ObjectsPath, ds.ObjectIdentifier(rel.Object).Key())
				if err != nil {
					return err
				}
				objects[ds.ObjectIdentifier(rel.Object).Key()] = obj
			}
			resp.Objects = objects
		}

		return nil
	})

	return resp, err
}

// Get relation instances based on subject, relation, object filter (paginated).
func (s *Directory) GetRelations(ctx context.Context, req *dsr.GetRelationsRequest) (*dsr.GetRelationsResponse, error) {
	resp := &dsr.GetRelationsResponse{Results: []*dsc.Relation{}, Page: &dsc.PaginationResponse{}}

	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{Size: 100}
	}

	if ok, err := ds.RelationSelector(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		results, page, err := ds.List[dsc.Relation](ctx, tx, ds.RelationsSubPath, req.Page)
		if err != nil {
			return err
		}

		resp.Results = results
		resp.Page = page

		return nil
	})

	return resp, err
}

// Check if subject has permission on object.
func (s *Directory) CheckPermission(ctx context.Context, req *dsr.CheckPermissionRequest) (*dsr.CheckPermissionResponse, error) {
	resp := &dsr.CheckPermissionResponse{}

	if ok, err := ds.CheckPermission(req).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		resp, err = ds.CheckPermission(req).Exec(ctx)
		return err
	})

	return resp, err
}

// Check if subject has relation to object.
func (s *Directory) CheckRelation(ctx context.Context, req *dsr.CheckRelationRequest) (*dsr.CheckRelationResponse, error) {
	resp := &dsr.CheckRelationResponse{}

	if ok, err := ds.CheckRelation(req).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		resp, err = ds.CheckRelation(req).Exec(ctx)
		return err
	})

	return resp, err
}

// Get object dependency graph.
func (s *Directory) GetGraph(ctx context.Context, req *dsr.GetGraphRequest) (*dsr.GetGraphResponse, error) {
	resp := &dsr.GetGraphResponse{}

	if ok, err := ds.GetGraph(req).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		results, err := ds.GetGraph(req).Exec(ctx, tx)
		if err != nil {
			return err
		}

		resp.Results = results
		return nil
	})

	return resp, err
}
