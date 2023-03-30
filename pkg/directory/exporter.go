package directory

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dse "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/types"
)

func (s *Directory) Export(req *dse.ExportRequest, stream dse.Exporter_ExportServer) (err error) {
	logger := s.logger.With().Str("method", "Export").Interface("req", req).Logger()

	txOpt, cleanup, err := s.store.ReadTxOpts()
	if err != nil {
		return err
	}
	defer func() {
		cErr := cleanup()
		if cErr != nil {
			err = cErr
		}
	}()

	sc := types.StoreContext{Context: stream.Context(), Store: s.store, Opts: []boltdb.Opts{txOpt}}

	if req.Options&uint32(dse.Option_OPTION_METADATA_OBJECT_TYPES) != 0 {
		if err := exportObjectTypes(&sc, stream); err != nil {
			logger.Error().Err(err).Msg("export_object_types")
			return err
		}
	}

	if req.Options&uint32(dse.Option_OPTION_METADATA_PERMISSIONS) != 0 {
		if err := exportPermissions(&sc, stream); err != nil {
			logger.Error().Err(err).Msg("export_permissions")
			return err
		}
	}

	if req.Options&uint32(dse.Option_OPTION_METADATA_RELATION_TYPES) != 0 {
		if err := exportRelationTypes(&sc, stream); err != nil {
			logger.Error().Err(err).Msg("export_relation_types")
			return err
		}
	}

	if req.Options&uint32(dse.Option_OPTION_DATA_OBJECTS) != 0 {
		if err := exportObjects(&sc, stream); err != nil {
			logger.Error().Err(err).Msg("export_objects")
			return err
		}
	}

	if req.Options&uint32(dse.Option_OPTION_DATA_RELATIONS) != 0 {
		if err := exportRelations(&sc, stream); err != nil {
			logger.Error().Err(err).Msg("export_relations")
			return err
		}
	}

	if req.Options&uint32(dse.Option_OPTION_DATA_RELATIONS_WITH_KEYS) != 0 {
		if err := exportRelationsWithKeys(&sc, stream); err != nil {
			logger.Error().Err(err).Msg("export_relations_with_keys")
			return err
		}
	}

	return nil
}

func exportObjectTypes(sc *types.StoreContext, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{
		Size:  100,
		Token: "",
	}

	for {
		objTypes, pageResp, err := sc.GetObjectTypes(page)
		if err != nil {
			return err
		}

		for _, objType := range objTypes {

			if err := stream.Send(&dse.ExportResponse{
				Msg: &dse.ExportResponse_ObjectType{
					ObjectType: objType,
				},
			}); err != nil {
				return err
			}
		}

		if pageResp.NextToken == "" {
			break
		}

		page.Token = pageResp.NextToken
	}
	return nil
}

func exportPermissions(sc *types.StoreContext, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{
		Size:  100,
		Token: "",
	}

	for {
		permissions, pageResp, err := sc.GetPermissions(page)
		if err != nil {
			return err
		}

		for _, permission := range permissions {

			if err := stream.Send(&dse.ExportResponse{
				Msg: &dse.ExportResponse_Permission{
					Permission: permission,
				},
			}); err != nil {
				return err
			}
		}

		if pageResp.NextToken == "" {
			break
		}

		page.Token = pageResp.NextToken
	}
	return nil
}

func exportRelationTypes(sc *types.StoreContext, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{
		Size:  100,
		Token: "",
	}

	for {
		relTypes, pageResp, err := sc.GetRelationTypes(&dsc.ObjectTypeIdentifier{}, page)
		if err != nil {
			return err
		}

		for _, relType := range relTypes {

			if err := stream.Send(&dse.ExportResponse{
				Msg: &dse.ExportResponse_RelationType{
					RelationType: relType,
				},
			}); err != nil {
				return err
			}
		}

		if pageResp.NextToken == "" {
			break
		}

		page.Token = pageResp.NextToken
	}
	return nil
}

func exportObjects(sc *types.StoreContext, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{
		Size:  100,
		Token: "",
	}

	for {
		objects, pageResp, err := sc.GetObjects(&dsc.ObjectTypeIdentifier{}, page)
		if err != nil {
			return err
		}

		for _, obj := range objects {

			if err := stream.Send(&dse.ExportResponse{
				Msg: &dse.ExportResponse_Object{
					Object: obj,
				},
			}); err != nil {
				return err
			}
		}

		if pageResp.NextToken == "" {
			break
		}

		page.Token = pageResp.NextToken
	}
	return nil
}

func exportRelations(sc *types.StoreContext, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{
		Size:  100,
		Token: "",
	}

	for {
		relations, pageResp, err := sc.GetRelations(&dsc.RelationIdentifier{}, page)
		if err != nil {
			return err
		}

		for _, rel := range relations {

			if err := stream.Send(&dse.ExportResponse{
				Msg: &dse.ExportResponse_Relation{
					Relation: rel,
				},
			}); err != nil {
				return err
			}
		}

		if pageResp.NextToken == "" {
			break
		}

		page.Token = pageResp.NextToken
	}
	return nil
}

// TODO this should be the main and only code path without IDs.
func exportRelationsWithKeys(sc *types.StoreContext, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{
		Size:  100,
		Token: "",
	}

	for {
		relations, pageResp, err := sc.GetRelations(&dsc.RelationIdentifier{}, page)
		if err != nil {
			return err
		}

		for _, rel := range relations {
			sub, err := sc.GetObject(&dsc.ObjectIdentifier{
				Type: rel.Subject.Type,
				Key:  rel.Subject.Key,
			})
			if err != nil {
				return err
			}

			obj, err := sc.GetObject(&dsc.ObjectIdentifier{
				Type: rel.Object.Type,
				Key:  rel.Object.Key,
			})
			if err != nil {
				return err
			}

			rel.Subject.Key = &sub.Key
			rel.Object.Key = &obj.Key

			if err := stream.Send(&dse.ExportResponse{
				Msg: &dse.ExportResponse_Relation{
					Relation: rel,
				},
			}); err != nil {
				return err
			}
		}

		if pageResp.NextToken == "" {
			break
		}

		page.Token = pageResp.NextToken
	}
	return nil
}
