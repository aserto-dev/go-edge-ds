package v2

import (
	"context"

	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"

	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Writer struct {
	logger *zerolog.Logger
	store  *bdb.BoltDB
}

func NewWriter(logger *zerolog.Logger, store *bdb.BoltDB) *Writer {
	return &Writer{
		logger: logger,
		store:  store,
	}
}

// object type metadata methods.
func (s *Writer) SetObjectType(ctx context.Context, req *dsw2.SetObjectTypeRequest) (*dsw2.SetObjectTypeResponse, error) {
	resp := &dsw2.SetObjectTypeResponse{}

	if ok, err := ds.ObjectType(req.ObjectType).Validate(); !ok {
		return resp, err
	}

	req.ObjectType.Hash = ds.ObjectType(req.ObjectType).Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.ObjectTypesPath, ds.ObjectType(req.ObjectType).Key(), req.ObjectType)
		if err != nil {
			return err
		}

		objType, err := bdb.Set(ctx, tx, bdb.ObjectTypesPath, ds.ObjectType(req.ObjectType).Key(), updReq)
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	if err := s.store.LoadModel(); err != nil {
		s.logger.Error().Err(err).Msg("model reload")
	}

	return resp, err
}

func (s *Writer) DeleteObjectType(ctx context.Context, req *dsw2.DeleteObjectTypeRequest) (*dsw2.DeleteObjectTypeResponse, error) {
	resp := &dsw2.DeleteObjectTypeResponse{}

	if ok, err := ds.ObjectTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := bdb.Delete(ctx, tx, bdb.ObjectTypesPath, ds.ObjectTypeIdentifier(req.Param).Key()); err != nil {
			return err
		}
		resp.Result = &emptypb.Empty{}
		return nil
	})

	if err := s.store.LoadModel(); err != nil {
		s.logger.Error().Err(err).Msg("model reload")
	}

	return resp, err
}

// relation type metadata methods.
func (s *Writer) SetRelationType(ctx context.Context, req *dsw2.SetRelationTypeRequest) (*dsw2.SetRelationTypeResponse, error) {
	resp := &dsw2.SetRelationTypeResponse{}

	if ok, err := ds.RelationType(req.RelationType).Validate(s.store.MC()); !ok {
		return resp, err
	}

	req.RelationType.Hash = ds.RelationType(req.RelationType).Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.RelationTypesPath, ds.RelationType(req.RelationType).Key(), req.RelationType)
		if err != nil {
			return err
		}

		relType, err := bdb.Set(ctx, tx, bdb.RelationTypesPath, ds.RelationType(req.RelationType).Key(), updReq)
		if err != nil {
			return err
		}

		resp.Result = relType
		return nil
	})

	if err := s.store.LoadModel(); err != nil {
		s.logger.Error().Err(err).Msg("model reload")
	}

	return resp, err
}

func (s *Writer) DeleteRelationType(ctx context.Context, req *dsw2.DeleteRelationTypeRequest) (*dsw2.DeleteRelationTypeResponse, error) {
	resp := &dsw2.DeleteRelationTypeResponse{}

	if ok, err := ds.RelationTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := bdb.Delete(ctx, tx, bdb.RelationTypesPath, ds.RelationTypeIdentifier(req.Param).Key()); err != nil {
			return err
		}
		resp.Result = &emptypb.Empty{}
		return nil
	})

	if err := s.store.LoadModel(); err != nil {
		s.logger.Error().Err(err).Msg("model reload")
	}

	return resp, err
}

// permission metadata methods.
func (s *Writer) SetPermission(ctx context.Context, req *dsw2.SetPermissionRequest) (*dsw2.SetPermissionResponse, error) {
	resp := &dsw2.SetPermissionResponse{}

	if ok, err := ds.Permission(req.Permission).Validate(); !ok {
		return resp, err
	}

	req.Permission.Hash = ds.Permission(req.Permission).Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.PermissionsPath, ds.Permission(req.Permission).Key(), req.Permission)
		if err != nil {
			return err
		}

		objType, err := bdb.Set(ctx, tx, bdb.PermissionsPath, ds.Permission(req.Permission).Key(), updReq)
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	if err := s.store.LoadModel(); err != nil {
		s.logger.Error().Err(err).Msg("model reload")
	}

	return resp, err
}

func (s *Writer) DeletePermission(ctx context.Context, req *dsw2.DeletePermissionRequest) (*dsw2.DeletePermissionResponse, error) {
	resp := &dsw2.DeletePermissionResponse{}

	if ok, err := ds.PermissionIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := bdb.Delete(ctx, tx, bdb.PermissionsPath, ds.PermissionIdentifier(req.Param).Key()); err != nil {
			return err
		}
		resp.Result = &emptypb.Empty{}
		return nil
	})

	if err := s.store.LoadModel(); err != nil {
		s.logger.Error().Err(err).Msg("model reload")
	}

	return resp, err
}

// object methods.
func (s *Writer) SetObject(ctx context.Context, req *dsw2.SetObjectRequest) (*dsw2.SetObjectResponse, error) {
	resp := &dsw2.SetObjectResponse{}

	// if ok, err := ds.Object(req.Object).Validate(s.store.MC()); !ok {
	// 	return resp, err
	// }

	// req.Object.Hash = ds.Object(req.Object).Hash()

	// err := s.store.DB().Update(func(tx *bolt.Tx) error {
	// 	updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.ObjectsPath, ds.Object(req.Object).Key(), req.Object)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	objType, err := bdb.Set(ctx, tx, bdb.ObjectsPath, ds.Object(req.Object).Key(), updReq)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	resp.Result = objType
	// 	return nil
	// })

	return resp, status.Error(codes.Unimplemented, "SetObject")
}

func (s *Writer) DeleteObject(ctx context.Context, req *dsw2.DeleteObjectRequest) (*dsw2.DeleteObjectResponse, error) {
	resp := &dsw2.DeleteObjectResponse{}

	// if ok, err := ds.ObjectIdentifier(req.Param).Validate(); !ok {
	// 	return resp, err
	// }

	// err := s.store.DB().Update(func(tx *bolt.Tx) error {
	// 	if err := bdb.Delete(ctx, tx, bdb.ObjectsPath, ds.ObjectIdentifier(req.Param).Key()); err != nil {
	// 		return err
	// 	}

	// 	if req.GetWithRelations() {
	// 		{
	// 			// incoming object relations of object instance (result.type == incoming.subject.type && result.key == incoming.subject.key)
	// 			iter, err := bdb.NewScanIterator[dsc2.Relation](ctx, tx, bdb.RelationsSubPath, bdb.WithKeyFilter(ds.ObjectIdentifier(req.Param).Key()+ds.InstanceSeparator))
	// 			if err != nil {
	// 				return err
	// 			}

	// 			for iter.Next() {
	// 				if err := bdb.Delete(ctx, tx, bdb.RelationsObjPath, ds.Relation(iter.Value()).ObjKey()); err != nil {
	// 					return err
	// 				}

	// 				if err := bdb.Delete(ctx, tx, bdb.RelationsSubPath, ds.Relation(iter.Value()).SubKey()); err != nil {
	// 					return err
	// 				}
	// 			}
	// 		}
	// 		{
	// 			// outgoing object relations of object instance (result.type == outgoing.object.type && result.key == outgoing.object.key)
	// 			iter, err := bdb.NewScanIterator[dsc2.Relation](ctx, tx, bdb.RelationsObjPath, bdb.WithKeyFilter(ds.ObjectIdentifier(req.Param).Key()+ds.InstanceSeparator))
	// 			if err != nil {
	// 				return err
	// 			}

	// 			for iter.Next() {
	// 				if err := bdb.Delete(ctx, tx, bdb.RelationsObjPath, ds.Relation(iter.Value()).ObjKey()); err != nil {
	// 					return err
	// 				}

	// 				if err := bdb.Delete(ctx, tx, bdb.RelationsSubPath, ds.Relation(iter.Value()).SubKey()); err != nil {
	// 					return err
	// 				}
	// 			}
	// 		}
	// 	}

	// 	resp.Result = &emptypb.Empty{}
	// 	return nil
	// })

	return resp, status.Error(codes.Unimplemented, "DeleteObject")
}

// relation methods.
func (s *Writer) SetRelation(ctx context.Context, req *dsw2.SetRelationRequest) (*dsw2.SetRelationResponse, error) {
	resp := &dsw2.SetRelationResponse{}

	// if ok, err := ds.Relation(req.Relation).Validate(s.store.MC()); !ok {
	// 	return resp, err
	// }

	// req.Relation.Hash = ds.Relation(req.Relation).Hash()

	// err := s.store.DB().Update(func(tx *bolt.Tx) error {
	// 	updReq, err := bdb.UpdateMetadata(ctx, tx, bdb.RelationsObjPath, ds.Relation(req.Relation).ObjKey(), req.Relation)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	objRel, err := bdb.Set(ctx, tx, bdb.RelationsObjPath, ds.Relation(req.Relation).ObjKey(), updReq)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	subRel, err := bdb.Set(ctx, tx, bdb.RelationsSubPath, ds.Relation(req.Relation).SubKey(), req.Relation)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	resp.Result = objRel
	// 	_ = subRel

	// 	return nil
	// })

	return resp, status.Error(codes.Unimplemented, "SetRelation")
}

func (s *Writer) DeleteRelation(ctx context.Context, req *dsw2.DeleteRelationRequest) (*dsw2.DeleteRelationResponse, error) {
	resp := &dsw2.DeleteRelationResponse{}

	// if ok, err := ds.RelationIdentifier(req.Param).Validate(); !ok {
	// 	return resp, err
	// }

	// err := s.store.DB().Update(func(tx *bolt.Tx) error {
	// 	if err := bdb.Delete(ctx, tx, bdb.RelationsObjPath, ds.RelationIdentifier(req.Param).ObjKey()); err != nil {
	// 		return err
	// 	}
	// 	if err := bdb.Delete(ctx, tx, bdb.RelationsSubPath, ds.RelationIdentifier(req.Param).SubKey()); err != nil {
	// 		return err
	// 	}
	// 	resp.Result = &emptypb.Empty{}
	// 	return nil
	// })

	return resp, status.Error(codes.Unimplemented, "DeleteRelation")
}
