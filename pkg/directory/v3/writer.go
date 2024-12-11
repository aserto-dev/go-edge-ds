package v3

import (
	"context"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"

	"github.com/bufbuild/protovalidate-go"
	"github.com/go-http-utils/headers"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Writer struct {
	dsw3.UnimplementedWriterServer
	logger    *zerolog.Logger
	store     *bdb.BoltDB
	validator *protovalidate.Validator
}

func NewWriter(logger *zerolog.Logger, store *bdb.BoltDB, validator *protovalidate.Validator) *Writer {
	return &Writer{
		logger:    logger,
		store:     store,
		validator: validator,
	}
}

func (s *Writer) Validate(msg proto.Message) error {
	return s.validator.Validate(msg)
}

// object methods.
func (s *Writer) SetObject(ctx context.Context, req *dsw3.SetObjectRequest) (*dsw3.SetObjectResponse, error) {
	resp := &dsw3.SetObjectResponse{}

	if err := s.Validate(req); err != nil {
		// invalid proto message.
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	obj := ds.Object(req.Object)
	if err := obj.Validate(s.store.MC()); err != nil {
		// The object violates the model.
		return resp, err
	}

	etag := obj.Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		updObj, err := ds.UpdateMetadataObject(ctx, tx, bdb.ObjectsPath, obj.Key(), req.Object)
		if err != nil {
			return err
		}

		// optimistic concurrency check
		ifMatchHeader := metautils.ExtractIncoming(ctx).Get(headers.IfMatch)
		// if the updReq.Etag == "" this means the this is an insert
		if ifMatchHeader != "" && updObj.Etag != "" && ifMatchHeader != updObj.Etag {
			return derr.ErrHashMismatch.Msgf("for object with type [%s] and id [%s]", updObj.Type, updObj.Id)
		}

		if etag == updObj.Etag {
			s.logger.Trace().Bytes("key", ds.Object(req.Object).Key()).Str("etag-equal", etag).Msg("set_object")
			resp.Result = updObj
			return nil
		}

		updObj.Etag = etag

		objType, err := bdb.Set(ctx, tx, bdb.ObjectsPath, obj.Key(), updObj)
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

	if err := s.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	objIdent := ds.ObjectIdentifier(&dsc3.ObjectIdentifier{ObjectType: req.GetObjectType(), ObjectId: req.GetObjectId()})

	if err := objIdent.Validate(s.store.MC()); err != nil {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		objIdent := ds.ObjectIdentifier(&dsc3.ObjectIdentifier{ObjectType: req.ObjectType, ObjectId: req.ObjectId})

		// optimistic concurrency check
		ifMatchHeader := metautils.ExtractIncoming(ctx).Get(headers.IfMatch)
		if ifMatchHeader != "" {
			obj := &dsc3.Object{Type: req.ObjectType, Id: req.ObjectId}
			updObj, err := ds.UpdateMetadataObject(ctx, tx, bdb.ObjectsPath, ds.Object(obj).Key(), obj)
			if err != nil {
				return err
			}

			if ifMatchHeader != updObj.Etag {
				return derr.ErrHashMismatch.Msgf("for object with type [%s] and id [%s]", updObj.Type, updObj.Id)
			}
		}

		if err := bdb.Delete(ctx, tx, bdb.ObjectsPath, objIdent.Key()); err != nil {
			return err
		}

		if req.GetWithRelations() {
			{
				// incoming object relations of object instance (result.type == incoming.subject.type && result.key == incoming.subject.key)
				iter, err := bdb.NewScanIterator[dsc3.Relation](
					ctx, tx, bdb.RelationsSubPath,
					bdb.WithKeyFilter(append(objIdent.Key(), ds.InstanceSeparator)),
				)
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
				iter, err := bdb.NewScanIterator[dsc3.Relation](
					ctx, tx, bdb.RelationsObjPath,
					bdb.WithKeyFilter(append(objIdent.Key(), ds.InstanceSeparator)),
				)
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

	if err := s.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	relation := ds.Relation(req.Relation)
	if err := relation.Validate(s.store.MC()); err != nil {
		return resp, err
	}

	etag := relation.Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		updRel, err := ds.UpdateMetadataRelation(ctx, tx, bdb.RelationsObjPath, relation.ObjKey(), req.Relation)
		if err != nil {
			return err
		}

		// optimistic concurrency check
		ifMatchHeader := metautils.ExtractIncoming(ctx).Get(headers.IfMatch)
		// if the updReq.Etag == "" this means the this is an insert
		if ifMatchHeader != "" && updRel.Etag != "" && ifMatchHeader != updRel.Etag {
			return derr.ErrHashMismatch.Msgf("for relation with objectType [%s], objectId [%s], relation [%s], subjectType [%s], SubjectId [%s]", updRel.ObjectType, updRel.ObjectId, updRel.Relation, updRel.SubjectType, updRel.SubjectId)
		}

		if etag == updRel.Etag {
			s.logger.Trace().Bytes("key", ds.Relation(req.Relation).ObjKey()).Str("etag-equal", etag).Msg("set_relation")
			resp.Result = updRel
			return nil
		}

		updRel.Etag = etag

		objRel, err := bdb.Set(ctx, tx, bdb.RelationsObjPath, relation.ObjKey(), updRel)
		if err != nil {
			return err
		}

		if _, err := bdb.Set(ctx, tx, bdb.RelationsSubPath, relation.SubKey(), updRel); err != nil {
			return err
		}

		resp.Result = objRel

		return nil
	})

	return resp, err
}

func (s *Writer) DeleteRelation(ctx context.Context, req *dsw3.DeleteRelationRequest) (*dsw3.DeleteRelationResponse, error) {
	resp := &dsw3.DeleteRelationResponse{}

	if err := s.Validate(req); err != nil {
		return resp, derr.ErrProtoValidate.Msg(err.Error())
	}

	rel := &dsc3.Relation{
		ObjectType:      req.ObjectType,
		ObjectId:        req.ObjectId,
		Relation:        req.Relation,
		SubjectType:     req.SubjectType,
		SubjectId:       req.SubjectId,
		SubjectRelation: req.SubjectRelation,
	}
	rid := ds.Relation(rel)
	if err := rid.Validate(s.store.MC()); err != nil {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		// optimistic concurrency check
		ifMatchHeader := metautils.ExtractIncoming(ctx).Get(headers.IfMatch)
		if ifMatchHeader != "" {
			updRel, err := ds.UpdateMetadataRelation(ctx, tx, bdb.RelationsObjPath, rid.ObjKey(), rel)
			if err != nil {
				return err
			}

			if ifMatchHeader != updRel.Etag {
				return derr.ErrHashMismatch.Msgf("for relation with objectType [%s], objectId [%s], relation [%s], subjectType [%s], SubjectId [%s]", rel.ObjectType, rel.ObjectId, rel.Relation, rel.SubjectType, rel.SubjectId)
			}
		}

		if err := bdb.Delete(ctx, tx, bdb.RelationsObjPath, rid.ObjKey()); err != nil {
			return err
		}

		if err := bdb.Delete(ctx, tx, bdb.RelationsSubPath, rid.SubKey()); err != nil {
			return err
		}

		resp.Result = &emptypb.Empty{}
		return nil
	})

	return resp, err
}
