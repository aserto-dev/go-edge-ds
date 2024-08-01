package v3

import (
	"context"
	"errors"
	"fmt"
	"io"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsi3 "github.com/aserto-dev/go-directory/aserto/directory/importer/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/bufbuild/protovalidate-go"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Importer struct {
	logger    *zerolog.Logger
	store     *bdb.BoltDB
	validator *protovalidate.Validator
}

const (
	object   string = "object"
	relation string = "relation"
)

type counters map[string]*dsi3.ImportCounter

func NewImporter(logger *zerolog.Logger, store *bdb.BoltDB, validator *protovalidate.Validator) *Importer {
	return &Importer{
		logger:    logger,
		store:     store,
		validator: validator,
	}
}

func (s *Importer) Validate(msg proto.Message) error {
	return s.validator.Validate(msg)
}

func (s *Importer) Import(stream dsi3.Importer_ImportServer) error {
	ctx := stream.Context()

	ctr := counters{
		object:   {Type: object},
		relation: {Type: relation},
	}

	s.store.DB().MaxBatchSize = s.store.Config().MaxBatchSize
	s.store.DB().MaxBatchDelay = s.store.Config().MaxBatchDelay

	importErr := s.store.DB().Batch(func(tx *bolt.Tx) error {
		for {
			select {
			case <-ctx.Done(): // exit if context is done
				return nil
			default:
			}

			req, err := stream.Recv()
			if err == io.EOF {
				s.logger.Trace().Msg("import stream EOF")
				for _, c := range ctr {
					_ = stream.Send(&dsi3.ImportResponse{Msg: &dsi3.ImportResponse_Counter{Counter: c}})
				}
				// backwards compatible response.
				return stream.Send(&dsi3.ImportResponse{
					Object:   ctr[object],
					Relation: ctr[relation],
				})
			}

			if err != nil {
				s.logger.Trace().Str("err", err.Error()).Msg("cannot receive req")
				continue
			}

			if err := s.handleImportRequest(ctx, tx, req, ctr); err != nil {
				if stat, ok := status.FromError(err); ok {
					status := &dsi3.ImportStatus{
						Code: uint32(stat.Code()),
						Msg:  stat.Message(),
						Req:  req,
					}

					if err := stream.Send(&dsi3.ImportResponse{Msg: &dsi3.ImportResponse_Status{Status: status}}); err != nil {
						s.logger.Err(err).Msg("failed to send import status")
					}
				}
			}
		}
	})

	return importErr
}

func (s *Importer) handleImportRequest(ctx context.Context, tx *bolt.Tx, req *dsi3.ImportRequest, ctr counters) (err error) {
	switch m := req.Msg.(type) {
	case *dsi3.ImportRequest_Object:
		if req.OpCode == dsi3.Opcode_OPCODE_SET {
			err = s.objectSetHandler(ctx, tx, m.Object)
			ctr[object] = updateCounter(ctr[object], req.OpCode, err)
			return err
		}

		if req.OpCode == dsi3.Opcode_OPCODE_DELETE {
			err = s.objectDeleteHandler(ctx, tx, m.Object)
			ctr[object] = updateCounter(ctr[object], req.OpCode, err)
			return err
		}

		if req.OpCode == dsi3.Opcode_OPCODE_DELETE_WITH_RELATIONS {
			err = s.objectDeleteWithRelationsHandler(ctx, tx, m.Object)
			ctr[object] = updateCounter(ctr[object], req.OpCode, err)
			return err
		}

		return derr.ErrUnknownOpCode.Msgf("%s - %d", req.OpCode.String(), int32(req.OpCode))

	case *dsi3.ImportRequest_Relation:
		if req.OpCode == dsi3.Opcode_OPCODE_SET {
			err = s.relationSetHandler(ctx, tx, m.Relation)
			ctr[relation] = updateCounter(ctr[relation], req.OpCode, err)
			return err
		}

		if req.OpCode == dsi3.Opcode_OPCODE_DELETE {
			err = s.relationDeleteHandler(ctx, tx, m.Relation)
			ctr[relation] = updateCounter(ctr[relation], req.OpCode, err)
			return err
		}

		if req.OpCode == dsi3.Opcode_OPCODE_DELETE_WITH_RELATIONS {
			return derr.ErrInvalidOpCode.Msgf("%s for type relation", req.OpCode.String())
		}

		return derr.ErrUnknownOpCode.Msgf("%s - %d", req.OpCode.String(), int32(req.OpCode))

	default:
		return derr.ErrUnknown.Msgf("import request")
	}
}

func (s *Importer) objectSetHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Object) error {
	s.logger.Debug().Interface("object", req).Msg("ImportObject")

	if req == nil {
		return derr.ErrInvalidObject.Msg("nil")
	}

	if err := s.Validate(req); err != nil {
		return protoValidateError(err)
	}

	obj := ds.Object(req)
	if err := obj.Validate(s.store.MC()); err != nil {
		return err
	}

	etag := obj.Hash()

	updReq, err := bdb.UpdateMetadataObject(ctx, tx, bdb.ObjectsPath, obj.Key(), req)
	if err != nil {
		return err
	}

	if etag == updReq.Etag {
		s.logger.Trace().Str("key", obj.Key()).Str("etag-equal", etag).Msg("ImportObject")
		return nil
	}

	updReq.Etag = etag

	if _, err := bdb.SetObject(ctx, tx, bdb.ObjectsPath, ds.Object(updReq).Key(), updReq); err != nil {
		return derr.ErrInvalidObject.Msg("set")
	}

	return nil
}

func (s *Importer) objectDeleteHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Object) error {
	s.logger.Debug().Interface("object", req).Msg("ImportObject")

	if req == nil {
		return derr.ErrInvalidObject.Msg("nil")
	}

	if err := s.Validate(req); err != nil {
		return protoValidateError(err)
	}

	obj := ds.Object(req)
	if err := obj.Validate(s.store.MC()); err != nil {
		return err
	}

	if err := bdb.Delete(ctx, tx, bdb.ObjectsPath, obj.Key()); err != nil {
		return derr.ErrInvalidObject.Msg("delete")
	}

	return nil
}

func (s *Importer) objectDeleteWithRelationsHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Object) error {
	s.logger.Debug().Interface("object", req).Msg("ImportObject")

	if req == nil {
		return derr.ErrInvalidObject.Msg("nil")
	}

	if err := s.Validate(req); err != nil {
		return protoValidateError(err)
	}

	obj := ds.Object(req)
	if err := obj.Validate(s.store.MC()); err != nil {
		return err
	}

	if err := bdb.Delete(ctx, tx, bdb.ObjectsPath, obj.Key()); err != nil {
		return derr.ErrInvalidObject.Msg("delete")
	}

	{
		// incoming object relations of object instance (result.type == incoming.subject.type && result.key == incoming.subject.key)
		iter, err := bdb.NewScanIterator[dsc3.Relation](ctx, tx, bdb.RelationsSubPath, bdb.WithKeyFilter(obj.Key()+ds.InstanceSeparator))
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
		iter, err := bdb.NewScanIterator[dsc3.Relation](ctx, tx, bdb.RelationsObjPath, bdb.WithKeyFilter(obj.Key()+ds.InstanceSeparator))
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

	return nil
}

func (s *Importer) relationSetHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("ImportRelation")

	if req == nil {
		return derr.ErrInvalidRelation.Msg("nil")
	}

	if err := s.Validate(req); err != nil {
		return protoValidateError(err)
	}

	rel := ds.Relation(req)
	if err := rel.Validate(s.store.MC()); err != nil {
		return err
	}

	etag := rel.Hash()

	updReq, err := bdb.UpdateMetadataRelation(ctx, tx, bdb.RelationsObjPath, rel.ObjKey(), req)
	if err != nil {
		return err
	}

	if etag == updReq.Etag {
		s.logger.Trace().Str("key", rel.ObjKey()).Str("etag-equal", etag).Msg("ImportRelation")
		return nil
	}

	updReq.Etag = etag

	if _, err := bdb.SetRelation(ctx, tx, bdb.RelationsObjPath, ds.Relation(updReq).ObjKey(), updReq); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
	}

	if _, err := bdb.SetRelation(ctx, tx, bdb.RelationsSubPath, ds.Relation(updReq).SubKey(), updReq); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
	}

	return nil
}

func (s *Importer) relationDeleteHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("ImportRelation")

	if req == nil {
		return derr.ErrInvalidRelation.Msg("nil")
	}

	if err := s.Validate(req); err != nil {
		return protoValidateError(err)
	}

	rel := ds.Relation(req)
	if err := rel.Validate(s.store.MC()); err != nil {
		return err
	}

	if err := bdb.Delete(ctx, tx, bdb.RelationsObjPath, rel.ObjKey()); err != nil {
		return derr.ErrInvalidRelation.Msg("delete")
	}

	if err := bdb.Delete(ctx, tx, bdb.RelationsSubPath, rel.SubKey()); err != nil {
		return derr.ErrInvalidRelation.Msg("delete")
	}

	return nil
}

func updateCounter(c *dsi3.ImportCounter, opCode dsi3.Opcode, err error) *dsi3.ImportCounter {
	c.Recv++
	if opCode == dsi3.Opcode_OPCODE_SET {
		c.Set++
	} else if opCode == dsi3.Opcode_OPCODE_DELETE {
		c.Delete++
	}
	if err != nil {
		c.Error++
	}
	return c
}

func protoValidateError(e error) error {
	err := derr.ErrProtoValidate

	var valErr *protovalidate.ValidationError
	if ok := errors.As(e, &valErr); ok {
		err.Message = fmt.Sprintf("validation error: %s (%s)",
			valErr.Violations[0].GetConstraintId(),
			valErr.Violations[0].GetMessage(),
		)
		return err
	}

	err.Message = e.Error()
	return err
}
