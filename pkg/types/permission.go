package types

import (
	"bytes"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	"github.com/aserto-dev/go-edge-ds/pkg/session"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Permission struct {
	*dsc.Permission
}

func (i *Permission) PreValidate() (bool, error) {
	if i.Permission == nil {
		return false, errors.Errorf("permission not instantiated")
	}
	if strings.TrimSpace(i.Permission.GetName()) == "" {
		return false, errors.Errorf("name cannot be empty")
	}
	return true, nil
}

func (i *Permission) Validate() (bool, error) {
	if i.Permission == nil {
		return false, errors.Errorf("permission not instantiated")
	}
	if strings.TrimSpace(i.Permission.GetId()) == "" {
		return false, errors.Errorf("permission id cannot be empty")
	}
	if !ID.IsValid(i.Permission.GetId()) {
		return false, errors.Errorf("invalid permission id")
	}
	if strings.TrimSpace(i.Permission.GetName()) == "" {
		return false, errors.Errorf("name cannot be empty")
	}
	return true, nil
}

func (i *Permission) Normalize() error {
	i.Id = strings.ToLower(i.GetId())
	return nil
}

func (i *Permission) Msg() *dsc.Permission {
	if i == nil || i.Permission == nil {
		return &dsc.Permission{}
	}
	return i.Permission
}

func (i *Permission) GetHash() (string, error) {
	h := fnv.New64a()
	h.Reset()

	if _, err := h.Write([]byte(i.GetId())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write([]byte(i.GetName())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash, err
	}

	return strconv.FormatUint(h.Sum64(), 10), nil
}

func (sc *StoreContext) GetPermission(permissionIdentifier *PermissionIdentifier) (*Permission, error) {
	var permID string

	if permissionIdentifier.GetId() != "" {
		permID = permissionIdentifier.GetId()
	} else if permissionIdentifier.GetName() != "" {
		var err error
		permID, err = sc.GetPermissionID(permissionIdentifier)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, derr.ErrInvalidArgument
	}

	buf, err := sc.Store.Read(PermissionsPath(), permID, sc.Opts)
	if err != nil {
		return nil, err
	}

	var perm dsc.Permission
	if err := pb.BufToProto(bytes.NewReader(buf), &perm); err != nil {
		return nil, err
	}

	return &Permission{
		Permission: &perm,
	}, nil
}

func (sc *StoreContext) GetPermissionID(permissionIdentifier *PermissionIdentifier) (string, error) {
	if permissionIdentifier.GetId() != "" {
		return permissionIdentifier.GetId(), nil
	}

	idBuf, err := sc.Store.Read(PermissionsNamePath(), permissionIdentifier.GetName(), sc.Opts)
	if err != nil {
		return "", err
	}

	return string(idBuf), nil
}

func (sc *StoreContext) GetPermissionName(permissionIdentifier *PermissionIdentifier) (string, error) {
	if permissionIdentifier.GetName() != "" {
		return permissionIdentifier.GetName(), nil
	}

	buf, err := sc.Store.Read(PermissionsPath(), permissionIdentifier.GetId(), sc.Opts)
	if err != nil {
		return "", err
	}

	var permission dsc.Permission
	if err := pb.BufToProto(bytes.NewReader(buf), &permission); err != nil {
		return "", err
	}

	return permission.GetName(), nil
}

func (sc *StoreContext) GetPermissions(page *PaginationRequest) ([]*Permission, *PaginationResponse, error) {
	_, values, nextToken, _, err := sc.Store.List(PermissionsPath(), page.Token, page.Size, sc.Opts)
	if err != nil {
		return nil, &PaginationResponse{}, err
	}

	permissions := []*Permission{}
	for i := 0; i < len(values); i++ {
		var permission dsc.Permission
		if err := pb.BufToProto(bytes.NewReader(values[i]), &permission); err != nil {
			return nil, nil, err
		}
		permissions = append(permissions, &Permission{&permission})
	}

	if err != nil {
		return nil, &PaginationResponse{}, err
	}

	return permissions, &PaginationResponse{&dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(permissions))}}, nil
}

// func (i *Permission) Set(ctx context.Context, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
func (sc *StoreContext) SetPermission(permission *Permission) (*Permission, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	if ok, err := permission.PreValidate(); !ok {
		return &Permission{}, err
	}

	curHash := ""
	current, err := sc.GetPermission(&PermissionIdentifier{&dsc.PermissionIdentifier{Name: &permission.Name}})
	if err == nil {
		curHash = current.Permission.Hash
		if permission.Id == "" {
			permission.Id = current.Id
		}
	} else if permission.Id == "" {
		permission.Id = ID.New()
	}

	if ok, err := permission.Validate(); !ok {
		return &Permission{}, err
	}

	if err := permission.Normalize(); err != nil {
		return &Permission{}, err
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		permission.Hash = curHash
	}

	if curHash != "" && curHash != permission.Hash {
		return &Permission{}, derr.ErrHashMismatch.Str("current", curHash).Str("incoming", permission.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		permission.CreatedAt = ts
	}
	permission.UpdatedAt = ts

	newHash, _ := permission.GetHash()
	permission.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		return permission, nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, permission); err != nil {
		return permission, err
	}

	if err := sc.Store.Write(PermissionsPath(), permission.GetId(), buf.Bytes(), sc.Opts); err != nil {
		return &Permission{}, err
	}
	if err := sc.Store.Write(PermissionsNamePath(), permission.GetName(), []byte(permission.GetId()), sc.Opts); err != nil {
		return &Permission{}, err
	}

	return permission, nil
}

func (sc *StoreContext) DeletePermission(permissionIdentifier *PermissionIdentifier) error {
	if ok, err := permissionIdentifier.Validate(); !ok {
		return err
	}

	current, err := sc.GetPermission(permissionIdentifier)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := sc.Store.DeleteKey(PermissionsNamePath(), current.GetName(), sc.Opts); err != nil {
		return err
	}

	if err := sc.Store.DeleteKey(PermissionsPath(), current.GetId(), sc.Opts); err != nil {
		return err
	}

	return nil
}
