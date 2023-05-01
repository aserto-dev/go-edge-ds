package directory

import (
	"context"
	"errors"
	"time"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/ds"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// object type metadata methods.
func (s *Directory) SetObjectType(ctx context.Context, req *dsw.SetObjectTypeRequest) (*dsw.SetObjectTypeResponse, error) {
	resp := &dsw.SetObjectTypeResponse{}

	if ok, err := ds.ObjectType(req.ObjectType).Validate(); !ok {
		return resp, err
	}

	req.ObjectType.Hash = ds.ObjectType(req.ObjectType).Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		objType, err := ds.Set(ctx, tx, ds.ObjectTypesPath, ds.ObjectType(req.ObjectType).Key(), req.ObjectType)
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

func (s *Directory) DeleteObjectType(ctx context.Context, req *dsw.DeleteObjectTypeRequest) (*dsw.DeleteObjectTypeResponse, error) {
	resp := &dsw.DeleteObjectTypeResponse{}

	if ok, err := ds.ObjectTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := ds.Delete(ctx, tx, ds.ObjectTypesPath, ds.ObjectTypeIdentifier(req.Param).Key()); err != nil {
			return err
		}
		resp.Result = &emptypb.Empty{}
		return nil
	})

	return resp, err
}

// relation type metadata methods.
func (s *Directory) SetRelationType(ctx context.Context, req *dsw.SetRelationTypeRequest) (*dsw.SetRelationTypeResponse, error) {
	resp := &dsw.SetRelationTypeResponse{}

	if ok, err := ds.RelationType(req.RelationType).Validate(); !ok {
		return resp, err
	}

	req.RelationType.Hash = ds.RelationType(req.RelationType).Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		relType, err := ds.Set(ctx, tx, ds.RelationTypesPath, ds.RelationType(req.RelationType).Key(), req.RelationType)
		if err != nil {
			return err
		}

		resp.Result = relType
		return nil
	})

	return resp, err
}

func (s *Directory) DeleteRelationType(ctx context.Context, req *dsw.DeleteRelationTypeRequest) (*dsw.DeleteRelationTypeResponse, error) {
	resp := &dsw.DeleteRelationTypeResponse{}

	if ok, err := ds.RelationTypeIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := ds.Delete(ctx, tx, ds.RelationTypesPath, ds.RelationTypeIdentifier(req.Param).Key()); err != nil {
			return err
		}
		resp.Result = &emptypb.Empty{}
		return nil
	})

	return resp, err
}

// permission metadata methods.
func (s *Directory) SetPermission(ctx context.Context, req *dsw.SetPermissionRequest) (*dsw.SetPermissionResponse, error) {
	resp := &dsw.SetPermissionResponse{}

	if ok, err := ds.Permission(req.Permission).Validate(); !ok {
		return resp, err
	}

	req.Permission.Hash = ds.Permission(req.Permission).Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		objType, err := ds.Set(ctx, tx, ds.PermissionsPath, ds.Permission(req.Permission).Key(), req.Permission)
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

func (s *Directory) DeletePermission(ctx context.Context, req *dsw.DeletePermissionRequest) (*dsw.DeletePermissionResponse, error) {
	resp := &dsw.DeletePermissionResponse{}

	if ok, err := ds.PermissionIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := ds.Delete(ctx, tx, ds.PermissionsPath, ds.PermissionIdentifier(req.Param).Key()); err != nil {
			return err
		}
		resp.Result = &emptypb.Empty{}
		return nil
	})

	return resp, err
}

// object methods.
func (s *Directory) SetObject(ctx context.Context, req *dsw.SetObjectRequest) (*dsw.SetObjectResponse, error) {
	resp := &dsw.SetObjectResponse{}

	if ok, err := ds.Object(req.Object).Validate(); !ok {
		return resp, err
	}

	req.Object.Hash = ds.Object(req.Object).Hash()

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		objType, err := ds.Set(ctx, tx, ds.ObjectsPath, ds.Object(req.Object).Key(), req.Object)
		if err != nil {
			return err
		}

		resp.Result = objType
		return nil
	})

	return resp, err
}

func (s *Directory) DeleteObject(ctx context.Context, req *dsw.DeleteObjectRequest) (*dsw.DeleteObjectResponse, error) {
	resp := &dsw.DeleteObjectResponse{}

	if ok, err := ds.ObjectIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := ds.Delete(ctx, tx, ds.ObjectsPath, ds.ObjectIdentifier(req.Param).Key()); err != nil {
			return err
		}
		resp.Result = &emptypb.Empty{}
		return nil
	})

	return resp, err
}

// relation methods.
func (s *Directory) SetRelation(ctx context.Context, req *dsw.SetRelationRequest) (*dsw.SetRelationResponse, error) {
	resp := &dsw.SetRelationResponse{}

	if ok, err := ds.Relation(req.Relation).Validate(); !ok {
		return resp, err
	}

	req.Relation.Hash = ds.Relation(req.Relation).Hash()

	ts := timestamppb.New(time.Now().UTC())
	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		cur, err := ds.Get(ctx, tx, ds.RelationsObjPath, ds.Relation(req.Relation).ObjKey(), &dsc.Relation{})
		switch {
		case errors.Is(err, boltdb.ErrKeyNotFound):
			req.Relation.CreatedAt = ts
		case err != nil:
			s.logger.Debug().Err(err)
		default:
			if req.Relation.Hash == cur.Hash {
				resp.Result = cur
				return nil
			}
			req.Relation.CreatedAt = cur.CreatedAt
		}

		req.Relation.UpdatedAt = ts

		objRel, err := ds.Set(ctx, tx, ds.RelationsObjPath, ds.Relation(req.Relation).ObjKey(), req.Relation)
		if err != nil {
			return err
		}

		subRel, err := ds.Set(ctx, tx, ds.RelationsSubPath, ds.Relation(req.Relation).SubKey(), req.Relation)
		if err != nil {
			return err
		}

		resp.Result = objRel
		_ = subRel

		return nil
	})

	return resp, err
}

func (s *Directory) DeleteRelation(ctx context.Context, req *dsw.DeleteRelationRequest) (*dsw.DeleteRelationResponse, error) {
	resp := &dsw.DeleteRelationResponse{}

	if ok, err := ds.RelationIdentifier(req.Param).Validate(); !ok {
		return resp, err
	}

	err := s.store.DB().Update(func(tx *bolt.Tx) error {
		if err := ds.Delete(ctx, tx, ds.RelationsObjPath, ds.RelationIdentifier(req.Param).ObjKey()); err != nil {
			return err
		}
		if err := ds.Delete(ctx, tx, ds.RelationsSubPath, ds.RelationIdentifier(req.Param).SubKey()); err != nil {
			return err
		}
		resp.Result = &emptypb.Empty{}
		return nil
	})

	return resp, err
}
