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

	if obj := req.GetObject(); obj != nil {
		err = s.objectHandler(ctx, tx, obj)
		res.Object = updateCounter(res.Object, req.OpCode, err)
	} else if rel := req.GetRelation(); rel != nil {
		err = s.relationHandler(ctx, tx, rel)
		res.Relation = updateCounter(res.Relation, req.OpCode, err)
	}

	return err
}

func (s *Importer) objectHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Object) error {
	s.logger.Debug().Interface("object", req).Msg("import_object")

	if req == nil {
		return derr.ErrInvalidObject.Msg("nil")
	}

	if err := s.v.Validate(req); err != nil {
		return derr.ErrInvalidObject.Msg(err.Error())
	}

	req.Etag = ds.Object(req).Hash()

	updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.ObjectsPath, ds.Object(req).Key(), req)
	if err != nil {
		return err
	}

	if _, err := bdb.Set(ctx, tx, bdb.ObjectsPath, ds.Object(updReq).Key(), updReq); err != nil {
		return derr.ErrInvalidObject.Msg("set")
	}

	return nil
}

func (s *Importer) relationHandler(ctx context.Context, tx *bolt.Tx, req *dsc3.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("import_relation")

	if req == nil {
		return derr.ErrInvalidRelation.Msg("nil")
	}

	if err := s.v.Validate(req); err != nil {
		return derr.ErrInvalidRelation.Msg(err.Error())
	}

	req.Etag = ds.Relation(req).Hash()

	updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.RelationsObjPath, ds.Relation(req).ObjKey(), req)
	if err != nil {
		return err
	}

	if _, err := bdb.Set(ctx, tx, bdb.RelationsObjPath, ds.Relation(updReq).ObjKey(), updReq); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
	}

	if _, err := bdb.Set(ctx, tx, bdb.RelationsSubPath, ds.Relation(updReq).SubKey(), updReq); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
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
