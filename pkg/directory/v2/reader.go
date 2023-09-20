package v2

import (
	"context"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Reader struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
}

func NewReader(logger *zerolog.Logger, store *bdb.BoltDB) *Reader {
	return &Reader{
		logger: logger,
		store:  store,
	}
}

// Get object type (metadata).
func (s *Reader) GetObjectType(ctx context.Context, req *dsr.GetObjectTypeRequest) (*dsr.GetObjectTypeResponse, error) {
	resp := &dsr.GetObjectTypeResponse{}

	if ok, err := ds.ObjectTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		objType, err := bdb.Get[dsc.ObjectType](ctx, tx, bdb.ObjectTypesPath, ds.ObjectTypeIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

// Get all objects types (metadata) (paginated).
func (s *Reader) GetObjectTypes(ctx context.Context, req *dsr.GetObjectTypesRequest) (*dsr.GetObjectTypesResponse, error) {
	resp := &dsr.GetObjectTypesResponse{Results: []*dsc.ObjectType{}}

	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{Size: 100}
	}

	opts := []bdb.ScanOption{
		bdb.WithPageSize(req.Page.Size),
		bdb.WithPageToken(req.Page.Token),
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewPageIterator[dsc.ObjectType](ctx, tx, bdb.ObjectTypesPath, opts...)
		if err != nil {
			return err
		}

		iter.Next()

		resp.Results = iter.Value()
		resp.Page = &dsc.PaginationResponse{
			NextToken:  iter.NextToken(),
			ResultSize: int32(len(resp.Results)),
		}

		return nil
	})

	return resp, err
}

// Get relation type (metadata).
func (s *Reader) GetRelationType(ctx context.Context, req *dsr.GetRelationTypeRequest) (*dsr.GetRelationTypeResponse, error) {
	resp := &dsr.GetRelationTypeResponse{}

	if ok, err := ds.RelationTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		relType, err := bdb.Get[dsc.RelationType](ctx, tx, bdb.RelationTypesPath, ds.RelationTypeIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = relType
		return nil
	})

	return resp, err
}

// Get all relation types, optionally filtered by object type (metadata) (paginated).
func (s *Reader) GetRelationTypes(ctx context.Context, req *dsr.GetRelationTypesRequest) (*dsr.GetRelationTypesResponse, error) {
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

	opts := []bdb.ScanOption{
		bdb.WithPageSize(req.Page.Size),
		bdb.WithPageToken(req.Page.Token),
		bdb.WithKeyFilter(req.Param.GetName()),
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewPageIterator[dsc.RelationType](ctx, tx, bdb.RelationTypesPath, opts...)
		if err != nil {
			return err
		}

		iter.Next()

		resp.Results = iter.Value()
		resp.Page = &dsc.PaginationResponse{
			NextToken:  iter.NextToken(),
			ResultSize: int32(len(resp.Results)),
		}

		return nil
	})

	return resp, err
}

// Get permission (metadata).
func (s *Reader) GetPermission(ctx context.Context, req *dsr.GetPermissionRequest) (*dsr.GetPermissionResponse, error) {
	resp := &dsr.GetPermissionResponse{}

	if ok, err := ds.PermissionIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		objType, err := bdb.Get[dsc.Permission](ctx, tx, bdb.PermissionsPath, ds.PermissionIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

// Get all permissions (metadata) (paginated).
func (s *Reader) GetPermissions(ctx context.Context, req *dsr.GetPermissionsRequest) (*dsr.GetPermissionsResponse, error) {
	resp := &dsr.GetPermissionsResponse{Results: []*dsc.Permission{}}

	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{Size: 100}
	}

	opts := []bdb.ScanOption{
		bdb.WithPageSize(req.Page.Size),
		bdb.WithPageToken(req.Page.Token),
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewPageIterator[dsc.Permission](ctx, tx, bdb.PermissionsPath, opts...)
		if err != nil {
			return err
		}

		iter.Next()

		resp.Results = iter.Value()
		resp.Page = &dsc.PaginationResponse{
			NextToken:  iter.NextToken(),
			ResultSize: int32(len(resp.Results)),
		}

		return nil
	})

	return resp, err
}

// Get single object instance.
func (s *Reader) GetObject(ctx context.Context, req *dsr.GetObjectRequest) (*dsr.GetObjectResponse, error) {
	resp := &dsr.GetObjectResponse{}

	if ok, err := ds.ObjectIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		obj, err := bdb.Get[dsc.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		if req.GetWithRelations() {
			// incoming object relations of object instance (result.type == incoming.subject.type && result.key == incoming.subject.key)
			incoming, err := bdb.Scan[dsc.Relation](ctx, tx, bdb.RelationsSubPath, ds.Object(obj).Key())
			if err != nil {
				return err
			}
			resp.Relations = append(resp.Relations, incoming...)

			// outgoing object relations of object instance (result.type == outgoing.object.type && result.key == outgoing.object.key)
			outgoing, err := bdb.Scan[dsc.Relation](ctx, tx, bdb.RelationsObjPath, ds.Object(obj).Key())
			if err != nil {
				return err
			}
			resp.Relations = append(resp.Relations, outgoing...)

			s.logger.Trace().Msg("get object with relations")
		}

		resp.Result = obj
		return nil
	})

	return resp, err
}

// Get multiple object instances by id or type+key, in a single request.
func (s *Reader) GetObjectMany(ctx context.Context, req *dsr.GetObjectManyRequest) (*dsr.GetObjectManyResponse, error) {
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
			obj, err := bdb.Get[dsc.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(i).Key())
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
func (s *Reader) GetObjects(ctx context.Context, req *dsr.GetObjectsRequest) (*dsr.GetObjectsResponse, error) {
	resp := &dsr.GetObjectsResponse{Results: []*dsc.Object{}, Page: &dsc.PaginationResponse{}}

	if req.Param == nil {
		req.Param = &dsc.ObjectTypeIdentifier{}
	}

	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{Size: 100}
	}

	if ok, err := ds.ObjectTypeSelector(req.Param).Validate(); !ok {
		return resp, err
	}

	opts := []bdb.ScanOption{
		bdb.WithPageSize(req.Page.Size),
		bdb.WithPageToken(req.Page.Token),
		bdb.WithKeyFilter(req.Param.GetName()),
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewPageIterator[dsc.Object](ctx, tx, bdb.ObjectsPath, opts...)
		if err != nil {
			return err
		}

		iter.Next()

		resp.Results = iter.Value()
		resp.Page = &dsc.PaginationResponse{
			NextToken:  iter.NextToken(),
			ResultSize: int32(len(resp.Results)),
		}

		return nil
	})

	return resp, err
}

// Get relation instances based on subject, relation, object filter.
func (s *Reader) GetRelation(ctx context.Context, req *dsr.GetRelationRequest) (*dsr.GetRelationResponse, error) {
	resp := &dsr.GetRelationResponse{Results: []*dsc.Relation{}, Objects: map[string]*dsc.Object{}}

	if ok, err := ds.RelationIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	path, filter, err := ds.RelationIdentifier(req.Param).PathAndFilter()
	if err != nil {
		return resp, err
	}

	err = s.store.DB().View(func(tx *bolt.Tx) error {
		relations, err := bdb.Scan[dsc.Relation](ctx, tx, path, filter)
		if err != nil {
			return err
		}

		if len(relations) == 0 {
			return bdb.ErrKeyNotFound
		}
		if len(relations) != 1 {
			return bdb.ErrMultipleResults
		}

		resp.Results = append(resp.Results, relations...)

		if req.GetWithObjects() {
			objects := map[string]*dsc.Object{}
			for i := 0; i < len(resp.Results); i++ {
				rel := resp.Results[i]
				sub, err := bdb.Get[dsc.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(rel.Subject).Key())
				if err != nil {
					return err
				}
				objects[ds.ObjectIdentifier(rel.Subject).Key()] = sub

				obj, err := bdb.Get[dsc.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(rel.Object).Key())
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
func (s *Reader) GetRelations(ctx context.Context, req *dsr.GetRelationsRequest) (*dsr.GetRelationsResponse, error) {
	resp := &dsr.GetRelationsResponse{Results: []*dsc.Relation{}, Page: &dsc.PaginationResponse{}}

	if req.Page == nil {
		req.Page = &dsc.PaginationRequest{Size: 100}
	}

	if req.Param == nil {
		req.Param = &dsc.RelationIdentifier{
			Object:   &dsc.ObjectIdentifier{},
			Relation: &dsc.RelationTypeIdentifier{},
			Subject:  &dsc.ObjectIdentifier{},
		}
	}

	if ok, err := ds.RelationSelector(req.Param).Validate(); !ok {
		return resp, err
	}

	path, keyFilter, valueFilter := ds.RelationSelector(req.Param).Filter()

	opts := []bdb.ScanOption{
		bdb.WithPageToken(req.Page.Token),
		bdb.WithKeyFilter(keyFilter),
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewScanIterator[dsc.Relation](ctx, tx, path, opts...)
		if err != nil {
			return err
		}

		for iter.Next() {
			if !valueFilter(iter.Value()) {
				continue
			}
			resp.Results = append(resp.Results, iter.Value())

			if req.Page.Size == int32(len(resp.Results)) {
				if iter.Next() {
					resp.Page.NextToken = iter.Key()
				}
				break
			}
		}

		resp.Page.ResultSize = int32(len(resp.Results))

		return nil
	})

	return resp, err
}

// Check if subject has permission on object.
func (s *Reader) CheckPermission(ctx context.Context, req *dsr.CheckPermissionRequest) (*dsr.CheckPermissionResponse, error) {
	resp := &dsr.CheckPermissionResponse{}

	if ok, err := ds.CheckPermission(req).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		resp, err = ds.CheckPermission(req).Exec(ctx, tx)
		return err
	})

	return resp, err
}

// Check if subject has relation to object.
func (s *Reader) CheckRelation(ctx context.Context, req *dsr.CheckRelationRequest) (*dsr.CheckRelationResponse, error) {
	resp := &dsr.CheckRelationResponse{}

	if ok, err := ds.CheckRelation(req).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		resp, err = ds.CheckRelation(req).Exec(ctx, tx)
		return err
	})

	return resp, err
}

// Get object dependency graph.
func (s *Reader) GetGraph(ctx context.Context, req *dsr.GetGraphRequest) (*dsr.GetGraphResponse, error) {
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
