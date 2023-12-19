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

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Importer struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	v      *protovalidate.Validator
}

func NewImporter(logger *zerolog.Logger, store *bdb.BoltDB) *Importer {
	v, _ := protovalidate.New()
	return &Importer{
		logger: logger,
		store:  store,
		v:      v,
	}
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
			req, err := stream.Recv()
			if err == io.EOF {
				s.logger.Debug().Interface("res", res).Msg("import stream EOF")
				return stream.Send(res)
			} else if err != nil {
				s.logger.Err(err).Msg("cannot receive req")
				return stream.Send(res)
			}

			if err := s.handleImportRequest(ctx, tx, req, res); err != nil {
				s.logger.Err(err).Msg("cannot handle load request")
				return stream.Send(res)
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
		}

		if req.OpCode == dsi3.Opcode_OPCODE_DELETE {
			err = s.objectDeleteHandler(ctx, tx, m.Object)
			res.Object = updateCounter(res.Object, req.OpCode, err)
		}

	case *dsi3.ImportRequest_Relation:
		if req.OpCode == dsi3.Opcode_OPCODE_SET {
			err = s.relationSetHandler(ctx, tx, m.Relation)
			res.Relation = updateCounter(res.Relation, req.OpCode, err)
		}

		if req.OpCode == dsi3.Opcode_OPCODE_DELETE {
			err = s.relationDeleteHandler(ctx, tx, m.Relation)
			res.Relation = updateCounter(res.Relation, req.OpCode, err)
		}
	}

	return err
}

func (s *Importer) objectSetHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Object) error {
	s.logger.Debug().Interface("object", req).Msg("ImportObject")

	if req == nil {
		return derr.ErrInvalidObject.Msg("nil")
	}

	if err := s.v.Validate(req); err != nil {
		return derr.ErrProtoValidate.Msg(err.Error())
	}

	etag := ds.Object(req).Hash()

	updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.ObjectsPath, ds.Object(req).Key(), req)
	if err != nil {
		return err
	}

	if etag == updReq.Etag {
		s.logger.Trace().Str("key", ds.Object(req).Key()).Str("etag-equal", etag).Msg("ImportObject")
		return nil
	}

	updReq.Etag = etag

	if _, err := bdb.Set(ctx, tx, bdb.ObjectsPath, ds.Object(updReq).Key(), updReq); err != nil {
		return derr.ErrInvalidObject.Msg("set")
	}

	return nil
}

func (s *Importer) objectDeleteHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Object) error {
	s.logger.Debug().Interface("object", req).Msg("ImportObject")

	if req == nil {
		return derr.ErrInvalidObject.Msg("nil")
	}

	if err := s.v.Validate(req); err != nil {
		return derr.ErrProtoValidate.Msg(err.Error())
	}

	if err := bdb.Delete(ctx, tx, bdb.ObjectsPath, ds.Object(req).Key()); err != nil {
		return derr.ErrInvalidObject.Msg("delete")
	}

	return nil
}

func (s *Importer) relationSetHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("ImportRelation")

	if req == nil {
		return derr.ErrInvalidRelation.Msg("nil")
	}

	if err := s.v.Validate(req); err != nil {
		return derr.ErrProtoValidate.Msg(err.Error())
	}

	if err := s.store.MC().ValidateRelation(req); err != nil {
		// The relation violates the model.
		return err
	}

	etag := ds.Relation(req).Hash()

	updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.RelationsObjPath, ds.Relation(req).ObjKey(), req)
	if err != nil {
		return err
	}

	if etag == updReq.Etag {
		s.logger.Trace().Str("key", ds.Relation(req).ObjKey()).Str("etag-equal", etag).Msg("ImportRelation")
		return nil
	}

	updReq.Etag = etag

	if _, err := bdb.Set(ctx, tx, bdb.RelationsObjPath, ds.Relation(updReq).ObjKey(), updReq); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
	}

	if _, err := bdb.Set(ctx, tx, bdb.RelationsSubPath, ds.Relation(updReq).SubKey(), updReq); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
	}

	return nil
}

func (s *Importer) relationDeleteHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("ImportRelation")

	if req == nil {
		return derr.ErrInvalidRelation.Msg("nil")
	}

	if err := s.v.Validate(req); err != nil {
		return derr.ErrProtoValidate.Msg(err.Error())
	}

	if err := bdb.Delete(ctx, tx, bdb.RelationsObjPath, ds.Relation(req).ObjKey()); err != nil {
		return derr.ErrInvalidRelation.Msg("delete")
	}

	if err := bdb.Delete(ctx, tx, bdb.RelationsSubPath, ds.Relation(req).SubKey()); err != nil {
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
