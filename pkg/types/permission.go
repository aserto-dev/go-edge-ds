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

func NewPermission(i *dsc.Permission) *Permission {
	if i == nil {
		return &Permission{Permission: &dsc.Permission{}}
	}
	return &Permission{Permission: i}
}

func (i *Permission) PreValidate() (bool, error) {
	if i.Permission == nil {
		return false, derr.ErrInvalidPermission
	}
	if strings.TrimSpace(i.Permission.GetName()) == "" {
		return false, derr.ErrInvalidArgument.Msg("name not set")
	}
	return true, nil
}

func (i *Permission) Validate() (bool, error) {
	if ok, err := i.PreValidate(); !ok {
		return ok, err
	}
	return true, nil
}

func (i *Permission) Normalize() error {
	// TODO: is permission name case-insensitive?
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

	if _, err := h.Write([]byte(i.GetName())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash, err
	}

	return strconv.FormatUint(h.Sum64(), 10), nil
}

func (sc *StoreContext) GetPermission(permissionIdentifier *PermissionIdentifier) (*Permission, error) {

	buf, err := sc.Store.Read(PermissionsPath(), permissionIdentifier.GetName(), sc.Opts)
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

func (sc *StoreContext) SetPermission(permission *Permission) (*Permission, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	if ok, err := permission.PreValidate(); !ok {
		return &Permission{}, err
	}

	curHash := ""
	current, err := sc.GetPermission(&PermissionIdentifier{&dsc.PermissionIdentifier{Name: &permission.Name}})
	if err == nil {
		curHash = current.Permission.Hash
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
		permission.CreatedAt = current.CreatedAt
		permission.UpdatedAt = current.UpdatedAt
		return permission, nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, permission); err != nil {
		return permission, err
	}

	if err := sc.Store.Write(PermissionsPath(), permission.GetName(), buf.Bytes(), sc.Opts); err != nil {
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

	if err := sc.Store.DeleteKey(PermissionsPath(), current.GetName(), sc.Opts); err != nil {
		return err
	}

	return nil
}
