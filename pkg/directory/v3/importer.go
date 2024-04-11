package v3

import (
	"context"
	"io"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsi3 "github.com/aserto-dev/go-directory/aserto/directory/importer/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/aserto-dev/go-edge-ds/pkg/session"
	"github.com/bufbuild/protovalidate-go"
	"google.golang.org/protobuf/proto"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Importer struct {
	logger    *zerolog.Logger
	store     *bdb.BoltDB
	validator *protovalidate.Validator
}

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
	res := &dsi3.ImportResponse{
		Object:   &dsi3.ImportCounter{},
		Relation: &dsi3.ImportCounter{},
	}

	ctx := session.ContextWithSessionID(stream.Context(), uuid.NewString())

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
				s.logger.Trace().Interface("res", res).Msg("import stream EOF")
				return stream.Send(res)
			}

			if err != nil {
				s.logger.Trace().Str("err", err.Error()).Msg("cannot receive req")
				continue
			}

			if err := s.handleImportRequest(ctx, tx, req, res); err != nil {
				s.logger.Err(err).Msg("cannot handle load request")
			}
		}
	})

	return importErr
}

func (s *Importer) handleImportRequest(ctx context.Context, tx *bolt.Tx, req *dsi3.ImportRequest, res *dsi3.ImportResponse) (err error) {

	switch m := req.Msg.(type) {
	case *dsi3.ImportRequest_Object:
		if req.OpCode == dsi3.Opcode_OPCODE_SET {
			err = s.objectSetHandler(ctx, tx, m.Object)
			res.Object = updateCounter(res.Object, req.OpCode, err)
			return err
		}

		if req.OpCode == dsi3.Opcode_OPCODE_DELETE {
			err = s.objectDeleteHandler(ctx, tx, m.Object)
			res.Object = updateCounter(res.Object, req.OpCode, err)
			return err
		}

		return derr.ErrUnknownOpCode.Msgf("%s - %d", req.OpCode.Enum().String, int32(req.OpCode))

	case *dsi3.ImportRequest_Relation:
		if req.OpCode == dsi3.Opcode_OPCODE_SET {
			err = s.relationSetHandler(ctx, tx, m.Relation)
			res.Relation = updateCounter(res.Relation, req.OpCode, err)
			return err
		}

		if req.OpCode == dsi3.Opcode_OPCODE_DELETE {
			err = s.relationDeleteHandler(ctx, tx, m.Relation)
			res.Relation = updateCounter(res.Relation, req.OpCode, err)
			return err
		}

		return derr.ErrUnknownOpCode.Msgf("%s - %d", req.OpCode.Enum().String, int32(req.OpCode))

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
		// invalid proto message
		return derr.ErrProtoValidate.Msg(err.Error())
	}

	obj := ds.Object(req)
	if err := obj.Validate(s.store.MC()); err != nil {
		// The object violates the model.
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
		return derr.ErrProtoValidate.Msg(err.Error())
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

func (s *Importer) relationSetHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("ImportRelation")

	if req == nil {
		return derr.ErrInvalidRelation.Msg("nil")
	}

	if err := s.Validate(req); err != nil {
		// invalid proto message
		return derr.ErrProtoValidate.Msg(err.Error())
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
		return derr.ErrProtoValidate.Msg(err.Error())
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
