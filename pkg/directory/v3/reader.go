package v3

import (
	"context"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-directory/pkg/prop"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/pkg/errors"

	"github.com/bufbuild/protovalidate-go"
	"github.com/go-http-utils/headers"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	grpcmd "google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type Reader struct {
	logger    *zerolog.Logger
	store     *bdb.BoltDB
	validator *protovalidate.Validator
}

func NewReader(logger *zerolog.Logger, store *bdb.BoltDB, validator *protovalidate.Validator) *Reader {
	return &Reader{
		logger:    logger,
		store:     store,
		validator: validator,
	}
}

func (s *Reader) Validate(msg proto.Message) error {
	return s.validator.Validate(msg)
}

// GetObject, get single object instance.
func (s *Reader) GetObject(ctx context.Context, req *dsr3.GetObjectRequest) (*dsr3.GetObjectResponse, error) {
	resp := &dsr3.GetObjectResponse{}

	if err := s.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	objIdent := ds.ObjectIdentifier(&dsc3.ObjectIdentifier{ObjectType: req.ObjectType, ObjectId: req.ObjectId})
	if err := objIdent.Validate(s.store.MC()); err != nil {
		return resp, err
	}

	// TODO handle pagination request.
	err := s.store.DB().View(func(tx *bolt.Tx) error {
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

	if err := s.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	// validate all object identifiers first.
	for _, i := range req.Param {
		if err := ds.ObjectIdentifier(i).Validate(s.store.MC()); err != nil {
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

	if err := s.Validate(req); err != nil {
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
		if err := ds.ObjectSelector(&dsc3.ObjectIdentifier{ObjectType: req.ObjectType}).Validate(s.store.MC()); err != nil {
			return resp, err
		}

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

	if err := s.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	getRelation := ds.GetRelation(req)
	if err := getRelation.Validate(s.store.MC()); err != nil {
		return resp, err
	}

	path, filter, err := getRelation.PathAndFilter()
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

	if err := s.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	if req.Page == nil {
		req.Page = &dsc3.PaginationRequest{Size: 100}
	}

	getRelations := ds.GetRelations(req)
	if err := getRelations.Validate(s.store.MC()); err != nil {
		return resp, err
	}

	path, keyFilter, valueFilter := getRelations.Filter()

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

func setContextWithReason(err error) *structpb.Struct {
	return &structpb.Struct{
		Fields: map[string]*structpb.Value{
			prop.Reason: structpb.NewStringValue(err.Error()),
		},
	}
}

// Check, if subject is permitted to access resource (object).
func (s *Reader) Check(ctx context.Context, req *dsr3.CheckRequest) (*dsr3.CheckResponse, error) {
	resp := &dsr3.CheckResponse{}

	if err := s.Validate(req); err != nil {
		resp.Check = false
		resp.Context = setContextWithReason(err)
		return resp, nil
	}

	check := ds.Check(req)
	if err := check.Validate(s.store.MC()); err != nil {
		resp.Check = false

		if err := errors.Unwrap(err); err != nil {
			resp.Context = setContextWithReason(err)
			return resp, nil
		}

		resp.Context = setContextWithReason(err)
		return resp, nil
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		if err := check.RelationIdentifiersExist(ctx, tx); err != nil {
			return err
		}

		var err error
		resp, err = check.Exec(ctx, tx, s.store.MC())
		return err
	})
	if err != nil {
		resp.Context = setContextWithReason(err)
	}

	return resp, nil
}

// CheckPermission, check if subject is permitted to access resource (object).
func (s *Reader) CheckPermission(ctx context.Context, req *dsr3.CheckPermissionRequest) (*dsr3.CheckPermissionResponse, error) {
	resp := &dsr3.CheckPermissionResponse{}

	if err := s.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	if err := ds.CheckPermission(req).Validate(s.store.MC()); err != nil {
		return resp, err
	}

	check := ds.Check(&dsr3.CheckRequest{
		ObjectType:  req.GetObjectType(),
		ObjectId:    req.GetObjectId(),
		Relation:    req.GetPermission(),
		SubjectType: req.GetSubjectType(),
		SubjectId:   req.GetSubjectId(),
		Trace:       req.GetTrace(),
	})

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		r, err := check.Exec(ctx, tx, s.store.MC())
		if err == nil {
			resp.Check = r.Check
			resp.Trace = r.Trace
		}
		return err
	})

	return resp, err
}

// CheckRelation, check if subject has the specified relation to a resource (object).
func (s *Reader) CheckRelation(ctx context.Context, req *dsr3.CheckRelationRequest) (*dsr3.CheckRelationResponse, error) {
	resp := &dsr3.CheckRelationResponse{}

	if err := s.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	if err := ds.CheckRelation(req).Validate(s.store.MC()); err != nil {
		return resp, err
	}

	check := ds.Check(&dsr3.CheckRequest{
		ObjectType:  req.GetObjectType(),
		ObjectId:    req.GetObjectId(),
		Relation:    req.GetRelation(),
		SubjectType: req.GetSubjectType(),
		SubjectId:   req.GetSubjectId(),
		Trace:       req.GetTrace(),
	})

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		r, err := check.Exec(ctx, tx, s.store.MC())
		if err == nil {
			resp.Check = r.Check
			resp.Trace = r.Trace
		}
		return err
	})

	return resp, err
}

// GetGraph, return graph of connected objects and relations for requested anchor subject/object.
func (s *Reader) GetGraph(ctx context.Context, req *dsr3.GetGraphRequest) (*dsr3.GetGraphResponse, error) {
	resp := &dsr3.GetGraphResponse{}

	if err := s.Validate(req); err != nil {
		return &dsr3.GetGraphResponse{}, derr.ErrProtoValidate.Msg(err.Error())
	}

	getGraph := ds.GetGraph(req)
	if err := getGraph.Validate(s.store.MC()); err != nil {
		return resp, err
	}

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		var err error
		results, err := getGraph.Exec(ctx, tx, s.store.MC())
		if err != nil {
			return err
		}

		resp = results
		return nil
	})

	return resp, err
}
