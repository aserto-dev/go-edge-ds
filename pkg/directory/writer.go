package directory

import (
	"context"

	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/types"
	"google.golang.org/protobuf/types/known/emptypb"
)

// object type metadata methods.
func (s *Directory) SetObjectType(ctx context.Context, req *dsw.SetObjectTypeRequest) (resp *dsw.SetObjectTypeResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	r, err := sc.SetObjectType(req.ObjectType)
	return &dsw.SetObjectTypeResponse{Result: r}, err
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	err = sc.DeleteObjectType(req.Param)
	return &dsw.DeleteObjectTypeResponse{Result: &emptypb.Empty{}}, err
}

// relation type metadata methods.
func (s *Directory) SetRelationType(ctx context.Context, req *dsw.SetRelationTypeRequest) (resp *dsw.SetRelationTypeResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	r, err := sc.SetRelationType(req.RelationType)
	return &dsw.SetRelationTypeResponse{Result: r}, err
}

func (s *Directory) DeleteRelationType(ctx context.Context, req *dsw.DeleteRelationTypeRequest) (resp *dsw.DeleteRelationTypeResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	err = sc.DeleteRelationType(req.Param)
	return &dsw.DeleteRelationTypeResponse{Result: &emptypb.Empty{}}, err
}

// permission metadata methods.
func (s *Directory) SetPermission(ctx context.Context, req *dsw.SetPermissionRequest) (resp *dsw.SetPermissionResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	perm, err := sc.SetPermission(req.Permission)
	return &dsw.SetPermissionResponse{Result: perm}, err
}

func (s *Directory) DeletePermission(ctx context.Context, req *dsw.DeletePermissionRequest) (resp *dsw.DeletePermissionResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	err = sc.DeletePermission(req.Param)
	return &dsw.DeletePermissionResponse{Result: &emptypb.Empty{}}, err
}

// object methods.
func (s *Directory) SetObject(ctx context.Context, req *dsw.SetObjectRequest) (resp *dsw.SetObjectResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	obj, err := sc.SetObject(req.Object)
	return &dsw.SetObjectResponse{Result: obj}, err
}

func (s *Directory) DeleteObject(ctx context.Context, req *dsw.DeleteObjectRequest) (resp *dsw.DeleteObjectResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	err = sc.DeleteObject(req.Param)
	return &dsw.DeleteObjectResponse{Result: &emptypb.Empty{}}, err
}

// relation methods.
func (s *Directory) SetRelation(ctx context.Context, req *dsw.SetRelationRequest) (resp *dsw.SetRelationResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	r, err := sc.SetRelation(req.Relation)
	return &dsw.SetRelationResponse{Result: r}, err
}

func (s *Directory) DeleteRelation(ctx context.Context, req *dsw.DeleteRelationRequest) (resp *dsw.DeleteRelationResponse, err error) {
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

	sc := types.StoreContext{Context: ctx, Store: s.store, Opts: []boltdb.Opts{txOpt}}
	err = sc.DeleteRelation(req.Param)
	return &dsw.DeleteRelationResponse{Result: &emptypb.Empty{}}, err
}
