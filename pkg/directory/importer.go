package directory

import (
	"io"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsi "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/types"
)

func (s *Directory) Import(stream dsi.Importer_ImportServer) error {
	res := &dsi.ImportResponse{
		ObjectType:   &dsi.ImportCounter{},
		Permission:   &dsi.ImportCounter{},
		RelationType: &dsi.ImportCounter{},
		Object:       &dsi.ImportCounter{},
		Relation:     &dsi.ImportCounter{},
	}

	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	sc := types.StoreContext{Context: stream.Context(), Store: s.store, Opts: []boltdb.Opts{txOpt}}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			s.logger.Debug().Interface("res", res).Msg("load user response")
			return stream.Send(res)
		} else if err != nil {
			s.logger.Err(err).Msg("cannot receive req")
			return stream.Send(res)
		}

		if err := s.handleImportRequest(&sc, req, res); err != nil {
			s.logger.Err(err).Msg("cannot handle load request")
			return stream.Send(res)
		}
	}
}

func (s *Directory) handleImportRequest(sc *types.StoreContext, req *dsi.ImportRequest, res *dsi.ImportResponse) (err error) {

	if objType := req.GetObjectType(); objType != nil {
		err = s.objectTypeHandler(sc, objType)
		res.ObjectType = updateCounter(res.ObjectType, req.OpCode, err)
	} else if perm := req.GetPermission(); perm != nil {
		err = s.permissionHandler(sc, perm)
		res.Permission = updateCounter(res.Permission, req.OpCode, err)
	} else if relType := req.GetRelationType(); relType != nil {
		err = s.relationTypeHandler(sc, relType)
		res.RelationType = updateCounter(res.RelationType, req.OpCode, err)
	} else if obj := req.GetObject(); obj != nil {
		err = s.objectHandler(sc, obj)
		res.Object = updateCounter(res.Object, req.OpCode, err)
	} else if rel := req.GetRelation(); rel != nil {
		err = s.relationHandler(sc, rel)
		res.Relation = updateCounter(res.Relation, req.OpCode, err)
	}

	return err
}

func (s *Directory) objectTypeHandler(sc *types.StoreContext, req *dsc.ObjectType) error {
	s.logger.Debug().Interface("objectType", req).Msg("import_object_type")

	if req == nil {
		return derr.ErrInvalidObjectType.Msg("nil")
	}

	_, err := sc.SetObjectType(&types.ObjectType{ObjectType: req})
	if err != nil {
		return derr.ErrInvalidObjectType.Msg("set")
	}

	return nil
}

func (s *Directory) permissionHandler(sc *types.StoreContext, req *dsc.Permission) error {
	s.logger.Debug().Interface("permission", req).Msg("import_permission")

	if req == nil {
		return derr.ErrInvalidPermission.Msg("nil")
	}

	if _, err := sc.SetPermission(&types.Permission{Permission: req}); err != nil {
		return derr.ErrInvalidPermission.Msg("set")
	}

	return nil
}

func (s *Directory) relationTypeHandler(sc *types.StoreContext, req *dsc.RelationType) error {
	s.logger.Debug().Interface("relationType", req).Msg("import_relation_type")

	if req == nil {
		return derr.ErrInvalidRelationType.Msg("nil")
	}

	if _, err := sc.SetRelationType(&types.RelationType{RelationType: req}); err != nil {
		return derr.ErrInvalidRelationType.Msg("set")
	}

	return nil
}

func (s *Directory) objectHandler(sc *types.StoreContext, req *dsc.Object) error {
	s.logger.Debug().Interface("object", req).Msg("import_object")

	if req == nil {
		return derr.ErrInvalidObject.Msg("nil")
	}

	if _, err := sc.SetObject(&types.Object{Object: req}); err != nil {
		return derr.ErrInvalidObject.Msg("set")
	}

	return nil
}

func (s *Directory) relationHandler(sc *types.StoreContext, req *dsc.Relation) error {
	s.logger.Debug().Interface("relation", req).Msg("import_relation")

	if req == nil {
		return derr.ErrInvalidRelation.Msg("nil")
	}

	if _, err := sc.SetRelation(&types.Relation{Relation: req}); err != nil {
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
