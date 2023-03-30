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

type permission struct {
	*dsc.Permission
}

func Permission(i *dsc.Permission) *permission { return &permission{i} }

func (i *permission) PreValidate() (bool, error) {
	if i.Permission == nil {
		return false, derr.ErrInvalidPermission
	}
	if strings.TrimSpace(i.Permission.GetName()) == "" {
		return false, derr.ErrInvalidArgument.Msg("name not set")
	}
	return true, nil
}

func (i *permission) Validate() (bool, error) {
	if ok, err := i.PreValidate(); !ok {
		return ok, err
	}
	return true, nil
}

func (i *permission) Normalize() error {
	// TODO: is permission name case-insensitive?
	return nil
}

func (i *permission) GetHash() (string, error) {
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

func (sc *StoreContext) GetPermission(pi *dsc.PermissionIdentifier) (*dsc.Permission, error) {

	buf, err := sc.Store.Read(PermissionsPath(), pi.GetName(), sc.Opts)
	if err != nil {
		return nil, err
	}

	var perm dsc.Permission
	if err := pb.BufToProto(bytes.NewReader(buf), &perm); err != nil {
		return nil, err
	}

	return &perm, nil
}

func (sc *StoreContext) GetPermissions(page *dsc.PaginationRequest) ([]*dsc.Permission, *dsc.PaginationResponse, error) {
	_, values, nextToken, _, err := sc.Store.List(PermissionsPath(), page.Token, page.Size, sc.Opts)
	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
	}

	permissions := []*dsc.Permission{}
	for i := 0; i < len(values); i++ {
		var permission dsc.Permission
		if err := pb.BufToProto(bytes.NewReader(values[i]), &permission); err != nil {
			return nil, nil, err
		}
		permissions = append(permissions, &permission)
	}

	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
	}

	return permissions, &dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(permissions))}, nil
}

func (sc *StoreContext) SetPermission(p *dsc.Permission) (*dsc.Permission, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	if ok, err := Permission(p).PreValidate(); !ok {
		return &dsc.Permission{}, err
	}

	curHash := ""
	current, err := sc.GetPermission(&dsc.PermissionIdentifier{Name: &p.Name})
	if err == nil {
		curHash = current.Hash
	}

	if ok, err := Permission(p).Validate(); !ok {
		return &dsc.Permission{}, err
	}

	if err := Permission(p).Normalize(); err != nil {
		return &dsc.Permission{}, err
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		p.Hash = curHash
	}

	if curHash != "" && curHash != p.Hash {
		return &dsc.Permission{}, derr.ErrHashMismatch.Str("current", curHash).Str("incoming", p.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		p.CreatedAt = ts
	}
	p.UpdatedAt = ts

	newHash, _ := Permission(p).GetHash()
	p.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		p.CreatedAt = current.CreatedAt
		p.UpdatedAt = current.UpdatedAt
		return p, nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, p); err != nil {
		return p, err
	}

	if err := sc.Store.Write(PermissionsPath(), p.GetName(), buf.Bytes(), sc.Opts); err != nil {
		return &dsc.Permission{}, err
	}

	return p, nil
}

func (sc *StoreContext) DeletePermission(pi *dsc.PermissionIdentifier) error {
	if ok, err := PermissionIdentifier(pi).Validate(); !ok {
		return err
	}

	current, err := sc.GetPermission(pi)
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
