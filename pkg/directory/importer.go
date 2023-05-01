package directory

import (
	"context"
	"io"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsi "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	"github.com/aserto-dev/go-edge-ds/pkg/session"
	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

func (s *Directory) Import(stream dsi.Importer_ImportServer) error {
	res := &dsi.ImportResponse{
		ObjectType:   &dsi.ImportCounter{},
		Permission:   &dsi.ImportCounter{},
		RelationType: &dsi.ImportCounter{},
		Object:       &dsi.ImportCounter{},
		Relation:     &dsi.ImportCounter{},
	}

	ctx := session.ContextWithSessionID(stream.Context(), uuid.NewString())

	importErr := s.store.DB().Update(func(tx *bolt.Tx) error {
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				s.logger.Debug().Interface("res", res).Msg("load user response")
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

func (s *Directory) handleImportRequest(ctx context.Context, tx *bolt.Tx, req *dsi.ImportRequest, res *dsi.ImportResponse) (err error) {

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

func (s *Directory) objectTypeHandler(ctx context.Context, tx *bolt.Tx, req *dsc.ObjectType) error {
	s.logger.Debug().Interface("objectType", req).Msg("import_object_type")

	if req == nil {
		return derr.ErrInvalidObjectType.Msg("nil")
	}

	if _, err := ds.Set(ctx, tx, ds.ObjectTypesPath, ds.ObjectType(req).Key(), req); err != nil {
		return derr.ErrInvalidObjectType.Msg("set")
	}

	return nil
}

func (s *Directory) permissionHandler(ctx context.Context, tx *bolt.Tx, req *dsc.Permission) error {
	s.logger.Debug().Interface("permission", req).Msg("import_permission")

	if req == nil {
		return derr.ErrInvalidPermission.Msg("nil")
	}

	if _, err := ds.Set(ctx, tx, ds.PermissionsPath, ds.Permission(req).Key(), req); err != nil {
		return derr.ErrInvalidPermission.Msg("set")
	}

	return nil
}

func (s *Directory) relationTypeHandler(ctx context.Context, tx *bolt.Tx, req *dsc.RelationType) error {
	s.logger.Debug().Interface("relationType", req).Msg("import_relation_type")

	if req == nil {
		return derr.ErrInvalidRelationType.Msg("nil")
	}

	if _, err := ds.Set(ctx, tx, ds.RelationTypesPath, ds.RelationType(req).Key(), req); err != nil {
		return derr.ErrInvalidRelationType.Msg("set")
	}

	return nil
}

func (s *Directory) objectHandler(ctx context.Context, tx *bolt.Tx, req *dsc.Object) error {
	s.logger.Debug().Interface("object", req).Msg("import_object")

	if req == nil {
		return derr.ErrInvalidObject.Msg("nil")
	}

	if _, err := ds.Set(ctx, tx, ds.ObjectsPath, ds.Object(req).Key(), req); err != nil {
		return derr.ErrInvalidObject.Msg("set")
	}

	return nil
}

func (s *Directory) relationHandler(ctx context.Context, tx *bolt.Tx, req *dsc.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("import_relation")

	if req == nil {
		return derr.ErrInvalidRelation.Msg("nil")
	}

	if _, err := ds.Set(ctx, tx, ds.RelationsObjPath, ds.Relation(req).Key(), req); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
	}

	if _, err := ds.Set(ctx, tx, ds.RelationsSubPath, ds.Relation(req).Key(), req); err != nil {
		return derr.ErrInvalidRelation.Msg("set")
	}

	return nil
}

func updateCounter(c *dsi.ImportCounter, opCode dsi.Opcode, err error) *dsi.ImportCounter {
	c.Recv++
	if opCode == dsi.Opcode_OPCODE_SET {
		c.Set++
	} else if opCode == dsi.Opcode_OPCODE_DELETE {
		c.Delete++
	}
	if err != nil {
		c.Error++
	}
	return c
}
