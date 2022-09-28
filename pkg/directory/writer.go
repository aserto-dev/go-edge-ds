package directory

import (
	"context"

	"github.com/aserto-dev/edge-ds/pkg/boltdb"
	"github.com/aserto-dev/edge-ds/pkg/types"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"google.golang.org/protobuf/types/known/emptypb"
)

// object type metadata methods
func (s *Directory) SetObjectType(ctx context.Context, req *dsw.SetObjectTypeRequest) (resp *dsw.SetObjectTypeResponse, err error) {
	s.logger.Trace().Msg("SetObjectType")

	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	objType := types.NewObjectType(req.ObjectType)
	err = objType.Set(ctx, s.store, []boltdb.Opts{txOpt}...)
	return &dsw.SetObjectTypeResponse{Result: objType.Msg()}, err
}

func (s *Directory) DeleteObjectType(ctx context.Context, req *dsw.DeleteObjectTypeRequest) (resp *dsw.DeleteObjectTypeResponse, err error) {
	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	err = types.DeleteObjectType(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	return &dsw.DeleteObjectTypeResponse{Result: &emptypb.Empty{}}, err
}

// relation type metadata methods
func (s *Directory) SetRelationType(ctx context.Context, req *dsw.SetRelationTypeRequest) (*dsw.SetRelationTypeResponse, error) {
	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	relType := types.NewRelationType(req.RelationType)
	err = relType.Set(ctx, s.store, []boltdb.Opts{txOpt}...)
	return &dsw.SetRelationTypeResponse{Result: relType.Msg()}, err
}

func (s *Directory) DeleteRelationType(ctx context.Context, req *dsw.DeleteRelationTypeRequest) (*dsw.DeleteRelationTypeResponse, error) {
	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	err = types.DeleteRelationType(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	return &dsw.DeleteRelationTypeResponse{Result: &emptypb.Empty{}}, err
}

// permission metadata methods
func (s *Directory) SetPermission(ctx context.Context, req *dsw.SetPermissionRequest) (*dsw.SetPermissionResponse, error) {
	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	perm := types.NewPermission(req.Permission)
	err = perm.Set(ctx, s.store, []boltdb.Opts{txOpt}...)
	return &dsw.SetPermissionResponse{Result: perm.Msg()}, err
}

func (s *Directory) DeletePermission(ctx context.Context, req *dsw.DeletePermissionRequest) (*dsw.DeletePermissionResponse, error) {
	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	err = types.DeletePermission(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	return &dsw.DeletePermissionResponse{Result: &emptypb.Empty{}}, err
}

// object methods
func (s *Directory) SetObject(ctx context.Context, req *dsw.SetObjectRequest) (*dsw.SetObjectResponse, error) {
	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	obj := types.NewObject(req.Object)
	err = obj.Set(ctx, s.store, []boltdb.Opts{txOpt}...)
	return &dsw.SetObjectResponse{Result: obj.Msg()}, err
}

func (s *Directory) DeleteObject(ctx context.Context, req *dsw.DeleteObjectRequest) (*dsw.DeleteObjectResponse, error) {
	txOpt, cleanup, err := s.store.WriteTxOpts()
	if err != nil {
		return nil, err
	}
	defer func() {
		cErr := cleanup(err)
		if cErr != nil {
			err = cErr
		}
	}()

	err = types.DeleteObject(ctx, req.Param, s.store, []boltdb.Opts{txOpt}...)
	return &dsw.DeleteObjectResponse{Result: &emptypb.Empty{}}, err
}

// relation methods
func (s *Directory) SetRelation(ctx context.Context, req *dsw.SetRelationRequest) (*dsw.SetRelationResponse, error) {
	return nil, nil
}

func (s *Directory) DeleteRelation(ctx context.Context, req *dsw.DeleteRelationRequest) (*dsw.DeleteRelationResponse, error) {
	return nil, nil
}
