package v3

import (
	"context"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/go-http-utils/headers"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	grpcmd "google.golang.org/grpc/metadata"

	"github.com/bufbuild/protovalidate-go"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Reader struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	v      *protovalidate.Validator
}

func NewReader(logger *zerolog.Logger, store *bdb.BoltDB) *Reader {
	v, _ := protovalidate.New()
	return &Reader{
		logger: logger,
		store:  store,
		v:      v,
	}
}

// GetObject, get single object instance.
func (s *Reader) GetObject(ctx context.Context, req *dsr3.GetObjectRequest) (*dsr3.GetObjectResponse, error) {
	resp := &dsr3.GetObjectResponse{}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	// TODO handle pagination request.
	err := s.store.DB().View(func(tx *bolt.Tx) error {
		objIdent := ds.ObjectIdentifier(&dsc3.ObjectIdentifier{ObjectType: req.ObjectType, ObjectId: req.ObjectId})
		obj, err := bdb.Get[dsc3.Object](ctx, tx, bdb.ObjectsPath, objIdent.Key())
		if err != nil {
			return err
		}

		inMD, _ := grpcmd.FromIncomingContext(ctx)
		// optimistic concurrency check
		if lo.Contains(inMD.Get(headers.IfNoneMatch), obj.Etag) {
			_ = grpc.SetHeader(ctx, grpcmd.Pairs("x-http-code", "304"))

			return nil
		}

		if req.GetWithRelations() {
			// incoming object relations of object instance (result.type == incoming.subject.type && result.key == incoming.subject.key)
			incoming, err := bdb.Scan[dsc3.Relation](ctx, tx, bdb.RelationsSubPath, ds.Object(obj).Key())
			if err != nil {
				return err
			}
			resp.Relations = append(resp.Relations, incoming...)

			// outgoing object relations of object instance (result.type == outgoing.object.type && result.key == outgoing.object.key)
			outgoing, err := bdb.Scan[dsc3.Relation](ctx, tx, bdb.RelationsObjPath, ds.Object(obj).Key())
			if err != nil {
				return err
			}
			resp.Relations = append(resp.Relations, outgoing...)

			s.logger.Trace().Msg("get object with relations")
		}

		resp.Result = obj

		// TODO set pagination response.
		resp.Page = &dsc3.PaginationResponse{}

		return nil
	})

	return resp, err
}

// GetObjectMany, get multiple object instances by type+id, in a single request.
func (s *Reader) GetObjectMany(ctx context.Context, req *dsr3.GetObjectManyRequest) (*dsr3.GetObjectManyResponse, error) {
	resp := &dsr3.GetObjectManyResponse{Results: []*dsc3.Object{}}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	// validate all object identifiers first.
	for _, i := range req.Param {
		if ok, err := ds.ObjectIdentifier(i).Validate(); !ok {
			return resp, err
		}
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		for _, i := range req.Param {
			obj, err := bdb.Get[dsc3.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(i).Key())
			if err != nil {
				return err
			}
			resp.Results = append(resp.Results, obj)
		}
		return nil
	})

	return resp, err
}

// GetObjects, gets (all) object instances, optionally filtered by object type, as a paginated array of objects.
func (s *Reader) GetObjects(ctx context.Context, req *dsr3.GetObjectsRequest) (*dsr3.GetObjectsResponse, error) {
	resp := &dsr3.GetObjectsResponse{Results: []*dsc3.Object{}, Page: &dsc3.PaginationResponse{}}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	if req.Page == nil {
		req.Page = &dsc3.PaginationRequest{Size: 100}
	}

	opts := []bdb.ScanOption{
		bdb.WithPageSize(req.Page.Size),
		bdb.WithPageToken(req.Page.Token),
	}

	if req.GetObjectType() != "" {
		opts = append(opts, bdb.WithKeyFilter(req.GetObjectType()+ds.TypeIDSeparator))
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewPageIterator[dsc3.Object](ctx, tx, bdb.ObjectsPath, opts...)
		if err != nil {
			return err
		}

		iter.Next()

		resp.Results = iter.Value()
		resp.Page = &dsc3.PaginationResponse{NextToken: iter.NextToken()}

		return nil
	})

	return resp, err
}

// GetRelation, get a single relation instance based on subject, relation, object filter.
func (s *Reader) GetRelation(ctx context.Context, req *dsr3.GetRelationRequest) (*dsr3.GetRelationResponse, error) {
	resp := &dsr3.GetRelationResponse{
		Result:  &dsc3.Relation{},
		Objects: map[string]*dsc3.Object{},
	}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	path, filter, err := ds.GetRelation(req).PathAndFilter()
	if err != nil {
		return resp, err
	}

	err = s.store.DB().View(func(tx *bolt.Tx) error {
		relations, err := bdb.Scan[dsc3.Relation](ctx, tx, path, filter)
		if err != nil {
			return err
		}

		if len(relations) == 0 {
			return bdb.ErrKeyNotFound
		}
		if len(relations) != 1 {
			return bdb.ErrMultipleResults
		}

		dbRel := relations[0]
		resp.Result = dbRel

		inMD, _ := grpcmd.FromIncomingContext(ctx)
		// optimistic concurrency check
		if lo.Contains(inMD.Get(headers.IfNoneMatch), dbRel.Etag) {
			_ = grpc.SetHeader(ctx, grpcmd.Pairs("x-http-code", "304"))

			return nil
		}

		if req.GetWithObjects() {
			objects := map[string]*dsc3.Object{}
			rel := ds.Relation(dbRel)

			sub, err := bdb.Get[dsc3.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(rel.Subject()).Key())
			if err != nil {
				sub = &dsc3.Object{Type: rel.SubjectType, Id: rel.SubjectId}
			}
			objects[ds.Object(sub).Key()] = sub

			obj, err := bdb.Get[dsc3.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(rel.Object()).Key())
			if err != nil {
				obj = &dsc3.Object{Type: rel.ObjectType, Id: rel.ObjectId}
			}
			objects[ds.Object(obj).Key()] = obj

			resp.Objects = objects
		}

		return nil
	})

	return resp, err
}

// GetRelations, gets paginated set of relation instances based on subject, relation, object filter.
func (s *Reader) GetRelations(ctx context.Context, req *dsr3.GetRelationsRequest) (*dsr3.GetRelationsResponse, error) {
	resp := &dsr3.GetRelationsResponse{
		Results: []*dsc3.Relation{},
		Objects: map[string]*dsc3.Object{},
		Page:    &dsc3.PaginationResponse{},
	}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	if req.Page == nil {
		req.Page = &dsc3.PaginationRequest{Size: 100}
	}

	path, keyFilter, valueFilter := ds.GetRelations(req).Filter()

	opts := []bdb.ScanOption{
		bdb.WithPageToken(req.Page.Token),
		bdb.WithKeyFilter(keyFilter),
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		iter, err := bdb.NewScanIterator[dsc3.Relation](ctx, tx, path, opts...)
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

		if req.GetWithObjects() {
			objects := map[string]*dsc3.Object{}

			for _, r := range resp.Results {
				rel := ds.Relation(r)

				sub, err := bdb.Get[dsc3.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(rel.Subject()).Key())
				if err != nil {
					sub = &dsc3.Object{Type: rel.SubjectType, Id: rel.SubjectId}
				}
				objects[ds.Object(sub).Key()] = sub

				obj, err := bdb.Get[dsc3.Object](ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(rel.Object()).Key())
				if err != nil {
					obj = &dsc3.Object{Type: rel.ObjectType, Id: rel.ObjectId}
				}
				objects[ds.Object(obj).Key()] = obj
			}

			resp.Objects = objects
		}

		return nil
	})

	return resp, err
}

// Check, if subject is permitted to access resource (object).
func (s *Reader) Check(ctx context.Context, req *dsr3.CheckRequest) (*dsr3.CheckResponse, error) {
	resp := &dsr3.CheckResponse{}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		resp, err = ds.Check(req).Exec(ctx, tx, s.store.MC())
		return err
	})

	return resp, err
}

// CheckPermission, check if subject is permitted to access resource (object).
func (s *Reader) CheckPermission(ctx context.Context, req *dsr3.CheckPermissionRequest) (*dsr3.CheckPermissionResponse, error) {
	resp := &dsr3.CheckPermissionResponse{}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		resp, err = ds.CheckPermission(req).Exec(ctx, tx, s.store.MC())
		return err
	})

	return resp, err
}

// CheckRelation, check if subject has the specified relation to a resource (object).
func (s *Reader) CheckRelation(ctx context.Context, req *dsr3.CheckRelationRequest) (*dsr3.CheckRelationResponse, error) {
	resp := &dsr3.CheckRelationResponse{}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		resp, err = ds.CheckRelation(req).Exec(ctx, tx, s.store.MC())
		return err
	})

	return resp, err
}

// GetGraph, return graph of connected objects and relations for requested anchor subject/object.
func (s *Reader) GetGraph(ctx context.Context, req *dsr3.GetGraphRequest) (*dsr3.GetGraphResponse, error) {
	resp := &dsr3.GetGraphResponse{}

	if err := s.v.Validate(req); err != nil {
		return &dsr3.GetGraphResponse{}, derr.ErrProtoValidate.Msg(err.Error())
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
