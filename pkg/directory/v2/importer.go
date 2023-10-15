package v2

import (
	"context"
	"io"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsi2 "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	dsi3 "github.com/aserto-dev/go-directory/aserto/directory/importer/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/convert"
	v3 "github.com/aserto-dev/go-edge-ds/pkg/directory/v3"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/aserto-dev/go-edge-ds/pkg/session"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Importer struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
	i3     dsi3.ImporterServer
}

func NewImporter(logger *zerolog.Logger, store *bdb.BoltDB, i3 *v3.Importer) *Importer {
	return &Importer{
		logger: logger,
		store:  store,
		i3:     i3,
	}
}

func (s *Importer) Import(stream dsi2.Importer_ImportServer) error {
	res := &dsi2.ImportResponse{
		ObjectType:   &dsi2.ImportCounter{},
		Permission:   &dsi2.ImportCounter{},
		RelationType: &dsi2.ImportCounter{},
		Object:       &dsi2.ImportCounter{},
		Relation:     &dsi2.ImportCounter{},
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

func (s *Importer) handleImportRequest(ctx context.Context, tx *bolt.Tx, req *dsi2.ImportRequest, res *dsi2.ImportResponse) (err error) {

	if obj := req.GetObject(); obj != nil {
		err = s.objectHandler(ctx, tx, obj)
		res.Object = updateCounter(res.Object, req.OpCode, err)
	} else if rel := req.GetRelation(); rel != nil {
		err = s.relationHandler(ctx, tx, rel)
		res.Relation = updateCounter(res.Relation, req.OpCode, err)
	}

	return err
}

func (s *Importer) objectHandler(ctx context.Context, tx *bolt.Tx, req *dsc2.Object) error {
	s.logger.Debug().Interface("object", req).Msg("import_object")

	req3 := convert.ObjectToV3(req)

	if req3 == nil {
		return derr.ErrInvalidObject.Msg("nil")
	}

	if ok, err := ds.Object(req3).Validate(s.store.MC()); !ok {
		return err
	}

	if _, err := bdb.Set[dsc3.Object](ctx, tx, bdb.ObjectsPath, ds.Object(req3).Key(), req3); err != nil {
		return derr.ErrInvalidObject.Msg("set")
	}

	return nil
}

func (s *Importer) relationHandler(ctx context.Context, tx *bolt.Tx, req *dsc2.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("import_relation")

	req3 := convert.RelationToV3(req)

	if req3 == nil {
		return derr.ErrInvalidRelation.Msg("nil")
	}

	if _, err := bdb.Set[dsc3.Relation](ctx, tx, bdb.RelationsObjPath, ds.Relation(req3).ObjKey(), req3); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
	}

	if _, err := bdb.Set[dsc3.Relation](ctx, tx, bdb.RelationsSubPath, ds.Relation(req3).SubKey(), req3); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
	}

	return nil
}

func updateCounter(c *dsi2.ImportCounter, opCode dsi2.Opcode, err error) *dsi2.ImportCounter {
	c.Recv++
	if opCode == dsi2.Opcode_OPCODE_SET {
		c.Set++
	} else if opCode == dsi2.Opcode_OPCODE_DELETE {
		c.Delete++
	}
	if err != nil {
		c.Error++
	}
	return c
}
