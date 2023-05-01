package directory

import (
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dse "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	bolt "go.etcd.io/bbolt"
)

func (s *Directory) Export(req *dse.ExportRequest, stream dse.Exporter_ExportServer) error {
	logger := s.logger.With().Str("method", "Export").Interface("req", req).Logger()

	err := s.store.DB().View(func(tx *bolt.Tx) error {
		if req.Options&uint32(dse.Option_OPTION_METADATA_OBJECT_TYPES) != 0 {
			if err := exportObjectTypes(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_object_types")
				return err
			}
		}

		if req.Options&uint32(dse.Option_OPTION_METADATA_PERMISSIONS) != 0 {
			if err := exportPermissions(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_permissions")
				return err
			}
		}

		if req.Options&uint32(dse.Option_OPTION_METADATA_RELATION_TYPES) != 0 {
			if err := exportRelationTypes(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_relation_types")
				return err
			}
		}

		if req.Options&uint32(dse.Option_OPTION_DATA_OBJECTS) != 0 {
			if err := exportObjects(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_objects")
				return err
			}
		}

		if req.Options&uint32(dse.Option_OPTION_DATA_RELATIONS) != 0 {
			if err := exportRelations(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_relations")
				return err
			}
		}

		if req.Options&uint32(dse.Option_OPTION_DATA_RELATIONS_WITH_KEYS) != 0 {
			if err := exportRelationsWithKeys(tx, stream); err != nil {
				logger.Error().Err(err).Msg("export_relations_with_keys")
				return err
			}
		}

		return nil
	})

	return err
}

func exportObjectTypes(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{Size: 100}

	for {
		objTypes, pageResp, err := ds.List(stream.Context(), tx, ds.ObjectTypesPath, &dsc.ObjectType{}, page)
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

func exportPermissions(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{Size: 100}

	for {
		permissions, pageResp, err := ds.List(stream.Context(), tx, ds.PermissionsPath, &dsc.Permission{}, page)
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

func exportRelationTypes(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{Size: 100}

	for {
		relTypes, pageResp, err := ds.List(stream.Context(), tx, ds.RelationTypesPath, &dsc.RelationType{}, page)
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

func exportObjects(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{Size: 100}

	for {
		objects, pageResp, err := ds.List(stream.Context(), tx, ds.ObjectsPath, &dsc.Object{}, page)
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

func exportRelations(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{Size: 100}

	for {
		relations, pageResp, err := ds.List(stream.Context(), tx, ds.RelationsSubPath, &dsc.Relation{}, page)
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
func exportRelationsWithKeys(tx *bolt.Tx, stream dse.Exporter_ExportServer) error {
	page := &dsc.PaginationRequest{Size: 100}

	for {
		relations, pageResp, err := ds.List(stream.Context(), tx, ds.RelationsSubPath, &dsc.Relation{}, page)

		if err != nil {
			return err
		}

		for _, rel := range relations {
			sub, err := ds.Get[dsc.Object](stream.Context(), tx, ds.ObjectsPath, ds.ObjectIdentifier(rel.Subject).Key())
			if err != nil {
				return err
			}

			obj, err := ds.Get[dsc.Object](stream.Context(), tx, ds.ObjectsPath, ds.ObjectIdentifier(rel.Object).Key())
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
