package v2

import (
	"context"
	"io"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsi2 "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/session"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
)

type Importer struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
}

func NewImporter(logger *zerolog.Logger, store *bdb.BoltDB) *Importer {
	return &Importer{
		logger: logger,
		store:  store,
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

	if res.ObjectType.Set != 0 || res.ObjectType.Delete != 0 ||
		res.RelationType.Set != 0 || res.RelationType.Delete != 0 ||
		res.Permission.Set != 0 || res.Permission.Delete != 0 {
		if err := s.store.LoadModel(); err != nil {
			s.logger.Error().Err(err).Msg("model reload")
		}
	}

	return importErr
}

func (s *Importer) handleImportRequest(ctx context.Context, tx *bolt.Tx, req *dsi2.ImportRequest, res *dsi2.ImportResponse) (err error) {

	if objType := req.GetObjectType(); objType != nil {
		err = s.objectTypeHandler(ctx, tx, objType)
		res.ObjectType = updateCounter(res.ObjectType, req.OpCode, err)
	} else if perm := req.GetPermission(); perm != nil {
		err = s.permissionHandler(ctx, tx, perm)
		res.Permission = updateCounter(res.Permission, req.OpCode, err)
	} else if relType := req.GetRelationType(); relType != nil {
		err = s.relationTypeHandler(ctx, tx, relType)
		res.RelationType = updateCounter(res.RelationType, req.OpCode, err)
	} else if obj := req.GetObject(); obj != nil {
		err = s.objectHandler(ctx, tx, obj)
		res.Object = updateCounter(res.Object, req.OpCode, err)
	} else if rel := req.GetRelation(); rel != nil {
		err = s.relationHandler(ctx, tx, rel)
		res.Relation = updateCounter(res.Relation, req.OpCode, err)
	}

	return err
}

func (s *Importer) objectTypeHandler(ctx context.Context, tx *bolt.Tx, req *dsc2.ObjectType) error {
	s.logger.Debug().Interface("objectType", req).Msg("import_object_type")

	// if req == nil {
	// 	return derr.ErrInvalidObjectType.Msg("nil")
	// }

	// if ok, err := ds.ObjectType(req).Validate(); !ok {
	// 	return err
	// }

	// if _, err := bdb.Set(ctx, tx, bdb.ObjectTypesPath, ds.ObjectType(req).Key(), req); err != nil {
	// 	return derr.ErrInvalidObjectType.Msg("set")
	// }

	return nil
}

func (s *Importer) permissionHandler(ctx context.Context, tx *bolt.Tx, req *dsc2.Permission) error {
	s.logger.Debug().Interface("permission", req).Msg("import_permission")

	// if req == nil {
	// 	return derr.ErrInvalidPermission.Msg("nil")
	// }

	// if ok, err := ds.Permission(req).Validate(); !ok {
	// 	return err
	// }

	// if _, err := bdb.Set(ctx, tx, bdb.PermissionsPath, ds.Permission(req).Key(), req); err != nil {
	// 	return derr.ErrInvalidPermission.Msg("set")
	// }

	return nil
}

func (s *Importer) relationTypeHandler(ctx context.Context, tx *bolt.Tx, req *dsc2.RelationType) error {
	s.logger.Debug().Interface("relationType", req).Msg("import_relation_type")

	// if req == nil {
	// 	return derr.ErrInvalidRelationType.Msg("nil")
	// }

	// if ok, err := ds.RelationType(req).Validate(s.store.MC()); !ok {
	// 	return err
	// }

	// if _, err := bdb.Set(ctx, tx, bdb.RelationTypesPath, ds.RelationType(req).Key(), req); err != nil {
	// 	return derr.ErrInvalidRelationType.Msg("set")
	// }

	return nil
}

func (s *Importer) objectHandler(ctx context.Context, tx *bolt.Tx, req *dsc2.Object) error {
	s.logger.Debug().Interface("object", req).Msg("import_object")

	// if req == nil {
	// 	return derr.ErrInvalidObject.Msg("nil")
	// }

	// if ok, err := ds.Object(req).Validate(s.store.MC()); !ok {
	// 	return err
	// }

	// if _, err := bdb.Set(ctx, tx, bdb.ObjectsPath, ds.Object(req).Key(), req); err != nil {
	// 	return derr.ErrInvalidObject.Msg("set")
	// }

	return nil
}

func (s *Importer) relationHandler(ctx context.Context, tx *bolt.Tx, req *dsc2.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("import_relation")

	// if req == nil {
	// 	return derr.ErrInvalidRelation.Msg("nil")
	// }

	// if ok, err := ds.Relation(req).Validate(s.store.MC()); !ok {
	// 	return err
	// }

	// if _, err := bdb.Set(ctx, tx, bdb.RelationsObjPath, ds.Relation(req).ObjKey(), req); err != nil {
	// 	return derr.ErrInvalidRelation.Msg("set")
	// }

	// if _, err := bdb.Set(ctx, tx, bdb.RelationsSubPath, ds.Relation(req).SubKey(), req); err != nil {
	// 	return derr.ErrInvalidRelation.Msg("set")
	// }

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
