package v2

import (
	"context"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
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
func (s *Reader) GetObjectType(ctx context.Context, req *dsr2.GetObjectTypeRequest) (*dsr2.GetObjectTypeResponse, error) {
	resp := &dsr2.GetObjectTypeResponse{}

	if ok, err := ds.ObjectTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		objType, err := bdb.Get[dsc2.ObjectType](ctx, tx, bdb.ObjectTypesPath, ds.ObjectTypeIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

// Get all objects types (metadata) (paginated).
func (s *Reader) GetObjectTypes(ctx context.Context, req *dsr2.GetObjectTypesRequest) (*dsr2.GetObjectTypesResponse, error) {
	resp := &dsr2.GetObjectTypesResponse{Results: []*dsc2.ObjectType{}}

	if req.Page == nil {
		req.Page = &dsc2.PaginationRequest{Size: 100}
	}

	opts := []bdb.ScanOption{
		bdb.WithPageSize(req.Page.Size),
		bdb.WithPageToken(req.Page.Token),
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewPageIterator[dsc2.ObjectType](ctx, tx, bdb.ObjectTypesPath, opts...)
		if err != nil {
			return err
		}

		iter.Next()

		resp.Results = iter.Value()
		resp.Page = &dsc2.PaginationResponse{
			NextToken:  iter.NextToken(),
			ResultSize: int32(len(resp.Results)),
		}

		return nil
	})

	return resp, err
}

// Get relation type (metadata).
func (s *Reader) GetRelationType(ctx context.Context, req *dsr2.GetRelationTypeRequest) (*dsr2.GetRelationTypeResponse, error) {
	resp := &dsr2.GetRelationTypeResponse{}

	if ok, err := ds.RelationTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		relType, err := bdb.Get[dsc2.RelationType](ctx, tx, bdb.RelationTypesPath, ds.RelationTypeIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = relType
		return nil
	})

	return resp, err
}

// Get all relation types, optionally filtered by object type (metadata) (paginated).
func (s *Reader) GetRelationTypes(ctx context.Context, req *dsr2.GetRelationTypesRequest) (*dsr2.GetRelationTypesResponse, error) {
	resp := &dsr2.GetRelationTypesResponse{Results: []*dsc2.RelationType{}, Page: &dsc2.PaginationResponse{}}

	if req.Param == nil {
		req.Param = &dsc2.ObjectTypeIdentifier{}
	}

	if ok, err := ds.ObjectTypeSelector(req.Param).Validate(); !ok {
		return resp, err
	}

	if req.Page == nil {
		req.Page = &dsc2.PaginationRequest{Size: 100}
	}

	opts := []bdb.ScanOption{
		bdb.WithPageSize(req.Page.Size),
		bdb.WithPageToken(req.Page.Token),
		bdb.WithKeyFilter(req.Param.GetName()),
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewPageIterator[dsc2.RelationType](ctx, tx, bdb.RelationTypesPath, opts...)
		if err != nil {
			return err
		}

		iter.Next()

		resp.Results = iter.Value()
		resp.Page = &dsc2.PaginationResponse{
			NextToken:  iter.NextToken(),
			ResultSize: int32(len(resp.Results)),
		}

		return nil
	})

	return resp, err
}

// Get permission (metadata).
func (s *Reader) GetPermission(ctx context.Context, req *dsr2.GetPermissionRequest) (*dsr2.GetPermissionResponse, error) {
	resp := &dsr2.GetPermissionResponse{}

	if ok, err := ds.PermissionIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		objType, err := bdb.Get[dsc2.Permission](ctx, tx, bdb.PermissionsPath, ds.PermissionIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

// Get all permissions (metadata) (paginated).
func (s *Reader) GetPermissions(ctx context.Context, req *dsr2.GetPermissionsRequest) (*dsr2.GetPermissionsResponse, error) {
	resp := &dsr2.GetPermissionsResponse{Results: []*dsc2.Permission{}}

	if req.Page == nil {
		req.Page = &dsc2.PaginationRequest{Size: 100}
	}

	opts := []bdb.ScanOption{
		bdb.WithPageSize(req.Page.Size),
		bdb.WithPageToken(req.Page.Token),
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewPageIterator[dsc2.Permission](ctx, tx, bdb.PermissionsPath, opts...)
		if err != nil {
			return err
		}

		iter.Next()

		resp.Results = iter.Value()
		resp.Page = &dsc2.PaginationResponse{
			NextToken:  iter.NextToken(),
			ResultSize: int32(len(resp.Results)),
		}

		return nil
	})

	return resp, err
}

// Get single object instance.
func (s *Reader) GetObject(ctx context.Context, req *dsr2.GetObjectRequest) (*dsr2.GetObjectResponse, error) {
	resp := &dsr2.GetObjectResponse{}

	if ok, err := ds.ObjectIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		obj, err := bdb.Get[dsc2.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(req.Param).Key())
		if err != nil {
			return err
		}

		if req.GetWithRelations() {
			// incoming object relations of object instance (result.type == incoming.subject.type && result.key == incoming.subject.key)
			incoming, err := bdb.Scan[dsc2.Relation](ctx, tx, bdb.RelationsSubPath, ds.Object(obj).Key())
			if err != nil {
				return err
			}
			resp.Relations = append(resp.Relations, incoming...)

			// outgoing object relations of object instance (result.type == outgoing.object.type && result.key == outgoing.object.key)
			outgoing, err := bdb.Scan[dsc2.Relation](ctx, tx, bdb.RelationsObjPath, ds.Object(obj).Key())
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
func (s *Reader) GetObjectMany(ctx context.Context, req *dsr2.GetObjectManyRequest) (*dsr2.GetObjectManyResponse, error) {
	resp := &dsr2.GetObjectManyResponse{Results: []*dsc2.Object{}}

	if req.Param == nil {
		req.Param = []*dsc2.ObjectIdentifier{}
	}

	// validate all object identifiers first.
	for _, i := range req.Param {
		if ok, err := ds.ObjectIdentifier(i).Validate(); !ok {
			return resp, err
		}
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		for _, i := range req.Param {
			obj, err := bdb.Get[dsc2.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(i).Key())
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
func (s *Reader) GetObjects(ctx context.Context, req *dsr2.GetObjectsRequest) (*dsr2.GetObjectsResponse, error) {
	resp := &dsr2.GetObjectsResponse{Results: []*dsc2.Object{}, Page: &dsc2.PaginationResponse{}}

	if req.Param == nil {
		req.Param = &dsc2.ObjectTypeIdentifier{}
	}

	if req.Page == nil {
		req.Page = &dsc2.PaginationRequest{Size: 100}
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
		iter, err := bdb.NewPageIterator[dsc2.Object](ctx, tx, bdb.ObjectsPath, opts...)
		if err != nil {
			return err
		}

		iter.Next()

		resp.Results = iter.Value()
		resp.Page = &dsc2.PaginationResponse{
			NextToken:  iter.NextToken(),
			ResultSize: int32(len(resp.Results)),
		}

		return nil
	})

	return resp, err
}

// Get relation instances based on subject, relation, object filter.
func (s *Reader) GetRelation(ctx context.Context, req *dsr2.GetRelationRequest) (*dsr2.GetRelationResponse, error) {
	resp := &dsr2.GetRelationResponse{Results: []*dsc2.Relation{}, Objects: map[string]*dsc2.Object{}}

	if ok, err := ds.RelationIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		rels, err := bdb.Scan[dsc2.Relation](ctx, tx, bdb.RelationsObjPath, ds.RelationIdentifier(req.Param).ObjKey())
		if err != nil {
			return err
		}

		if len(rels) == 0 {
			return bdb.ErrKeyNotFound
		}
		if len(rels) != 1 {
			return bdb.ErrMultipleResults
		}

		rel := rels[0]
		resp.Results = append(resp.Results, rel)

		if req.GetWithObjects() {
			objects := map[string]*dsc2.Object{}
			for i := 0; i < len(resp.Results); i++ {
				sub, err := bdb.Get[dsc2.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(rel.Subject).Key())
				if err != nil {
					return err
				}
				objects[ds.ObjectIdentifier(rel.Subject).Key()] = sub

				obj, err := bdb.Get[dsc2.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(rel.Object).Key())
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
func (s *Reader) GetRelations(ctx context.Context, req *dsr2.GetRelationsRequest) (*dsr2.GetRelationsResponse, error) {
	resp := &dsr2.GetRelationsResponse{Results: []*dsc2.Relation{}, Page: &dsc2.PaginationResponse{}}

	if req.Page == nil {
		req.Page = &dsc2.PaginationRequest{Size: 100}
	}

	if req.Param == nil {
		req.Param = &dsc2.RelationIdentifier{
			Object:   &dsc2.ObjectIdentifier{},
			Relation: &dsc2.RelationTypeIdentifier{},
			Subject:  &dsc2.ObjectIdentifier{},
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
		iter, err := bdb.NewScanIterator[dsc2.Relation](ctx, tx, path, opts...)
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
func (s *Reader) CheckPermission(ctx context.Context, req *dsr2.CheckPermissionRequest) (*dsr2.CheckPermissionResponse, error) {
	resp := &dsr2.CheckPermissionResponse{}

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
func (s *Reader) CheckRelation(ctx context.Context, req *dsr2.CheckRelationRequest) (*dsr2.CheckRelationResponse, error) {
	resp := &dsr2.CheckRelationResponse{}

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
func (s *Reader) GetGraph(ctx context.Context, req *dsr2.GetGraphRequest) (*dsr2.GetGraphResponse, error) {
	resp := &dsr2.GetGraphResponse{}

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
