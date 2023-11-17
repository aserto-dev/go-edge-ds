package v3

import (
	"context"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/bufbuild/protovalidate-go"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Writer struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	v      *protovalidate.Validator
}

func NewWriter(logger *zerolog.Logger, store *bdb.BoltDB) *Writer {
	v, _ := protovalidate.New()
	return &Writer{
		logger: logger,
		store:  store,
		v:      v,
	}
}

// object methods.
func (s *Writer) SetObject(ctx context.Context, req *dsw3.SetObjectRequest) (*dsw3.SetObjectResponse, error) {
	resp := &dsw3.SetObjectResponse{}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	etag := ds.Object(req.Object).Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.ObjectsPath, ds.Object(req.Object).Key(), req.Object)
		if err != nil {
			return err
		}

		if etag == updReq.Etag {
			s.logger.Trace().Str("key", ds.Object(req.Object).Key()).Str("etag-equal", etag).Msg("set_object")
			resp.Result = updReq
			return nil
		}

		updReq.Etag = etag

		objType, err := bdb.Set(ctx, tx, bdb.ObjectsPath, ds.Object(req.Object).Key(), updReq)
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

func (s *Writer) DeleteObject(ctx context.Context, req *dsw3.DeleteObjectRequest) (*dsw3.DeleteObjectResponse, error) {
	resp := &dsw3.DeleteObjectResponse{}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		objIdent := &dsc3.ObjectIdentifier{ObjectType: req.GetObjectType(), ObjectId: req.GetObjectId()}
		if err := bdb.Delete(ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(objIdent).Key()); err != nil {
			return err
		}

		if req.GetWithRelations() {
			{
				// incoming object relations of object instance (result.type == incoming.subject.type && result.key == incoming.subject.key)
				iter, err := bdb.NewScanIterator[dsc3.Relation](ctx, tx, bdb.RelationsSubPath, bdb.WithKeyFilter(ds.ObjectIdentifier(objIdent).Key()+ds.InstanceSeparator))
				if err != nil {
					return err
				}

				for iter.Next() {
					rel := ds.Relation(iter.Value())
					if err := bdb.Delete(ctx, tx, bdb.RelationsObjPath, rel.ObjKey()); err != nil {
						return err
					}

					if err := bdb.Delete(ctx, tx, bdb.RelationsSubPath, rel.SubKey()); err != nil {
						return err
					}
				}
			}
			{
				// outgoing object relations of object instance (result.type == outgoing.object.type && result.key == outgoing.object.key)
				iter, err := bdb.NewScanIterator[dsc3.Relation](ctx, tx, bdb.RelationsObjPath, bdb.WithKeyFilter(ds.ObjectIdentifier(objIdent).Key()+ds.InstanceSeparator))
				if err != nil {
					return err
				}

				for iter.Next() {
					rel := ds.Relation(iter.Value())

					if err := bdb.Delete(ctx, tx, bdb.RelationsObjPath, rel.ObjKey()); err != nil {
						return err
					}

					if err := bdb.Delete(ctx, tx, bdb.RelationsSubPath, rel.SubKey()); err != nil {
						return err
					}
				}
			}
		}

		resp.Result = &emptypb.Empty{}
		return nil
	})

	return resp, err
}

// relation methods.
func (s *Writer) SetRelation(ctx context.Context, req *dsw3.SetRelationRequest) (*dsw3.SetRelationResponse, error) {
	resp := &dsw3.SetRelationResponse{}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	etag := ds.Relation(req.Relation).Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.RelationsObjPath, ds.Relation(req.Relation).ObjKey(), req.Relation)
		if err != nil {
			return err
		}

		if etag == updReq.Etag {
			s.logger.Trace().Str("key", ds.Relation(req.Relation).ObjKey()).Str("etag-equal", etag).Msg("set_relation")
			resp.Result = updReq
			return nil
		}

		updReq.Etag = etag

		objRel, err := bdb.Set(ctx, tx, bdb.RelationsObjPath, ds.Relation(req.Relation).ObjKey(), updReq)
		if err != nil {
			return err
		}

		if _, err := bdb.Set(ctx, tx, bdb.RelationsSubPath, ds.Relation(req.Relation).SubKey(), updReq); err != nil {
			return err
		}

		resp.Result = objRel

		return nil
	})

	return resp, err
}

func (s *Writer) DeleteRelation(ctx context.Context, req *dsw3.DeleteRelationRequest) (*dsw3.DeleteRelationResponse, error) {
	resp := &dsw3.DeleteRelationResponse{}

	if err := s.v.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		rel := ds.Relation(&dsc3.Relation{
			ObjectType:      req.ObjectType,
			ObjectId:        req.ObjectId,
			Relation:        req.Relation,
			SubjectType:     req.SubjectType,
			SubjectId:       req.SubjectId,
			SubjectRelation: req.SubjectRelation,
		})

		if err := bdb.Delete(ctx, tx, bdb.RelationsObjPath, rel.ObjKey()); err != nil {
			return err
		}

		if err := bdb.Delete(ctx, tx, bdb.RelationsSubPath, rel.SubKey()); err != nil {
			return err
		}

		resp.Result = &emptypb.Empty{}
		return nil
	})

	return resp, err
}
