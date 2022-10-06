package types

import (
	"bytes"
	"context"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/aserto-dev/edge-ds/pkg/boltdb"
	"github.com/aserto-dev/edge-ds/pkg/pb"
	"github.com/aserto-dev/edge-ds/pkg/session"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-utils/cerr"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pkg/errors"
)

type Permission struct {
	*dsc.Permission
}

func NewPermission(i *dsc.Permission) *Permission {
	return &Permission{
		Permission: i,
	}
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

func GetPermission(ctx context.Context, i *dsc.PermissionIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (*Permission, error) {
	var permID string
	if i.GetId() != "" {
		permID = i.GetId()
	} else if i.GetName() != "" {
		idBuf, err := store.Read(PermissionsNamePath(), i.GetName(), opts)
		if err != nil {
			return nil, boltdb.ErrKeyNotFound
		}
		permID = string(idBuf)
	} else {
		return nil, cerr.ErrInvalidArgument
	}

	buf, err := store.Read(PermissionsPath(), permID, opts)
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

func (i *Permission) Set(ctx context.Context, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	sessionID := session.ExtractSessionID(ctx)

	if ok, err := i.Validate(); !ok {
		return err
	}
	if err := i.Normalize(); err != nil {
		return err
	}

	curHash := ""
	current, err := GetPermission(ctx, &dsc.PermissionIdentifier{Name: &i.Name}, store, opts...)
	if err == nil {
		curHash = current.Permission.Hash
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" /*&& i.Permission.Hash == "" */ {
		i.Permission.Hash = curHash
	}

	if curHash != i.Permission.Hash {
		return derr.ErrHashMismatch.Str("current", curHash).Str("incoming", i.Permission.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		i.Permission.CreatedAt = ts
	}
	i.Permission.UpdatedAt = ts

	newHash, _ := i.Hash()
	i.Permission.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		return nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, i.Permission); err != nil {
		return err
	}

	if err := store.Write(PermissionsPath(), i.GetId(), buf.Bytes(), opts); err != nil {
		return err
	}
	if err := store.Write(PermissionsNamePath(), i.Name, []byte(i.GetId()), opts); err != nil {
		return err
	}

	return nil
}

func DeletePermission(ctx context.Context, i *dsc.PermissionIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	if ok, err := PermissionIdentifier.Validate(i); !ok {
		return err
	}

	current, err := GetPermission(ctx, i, store, opts...)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := store.DeleteKey(PermissionsNamePath(), current.GetName(), opts); err != nil {
		return err
	}

	if err := store.DeleteKey(PermissionsPath(), current.GetId(), opts); err != nil {
		return err
	}

	return nil
}

func (i *Permission) Msg() *dsc.Permission {
	if i == nil || i.Permission == nil {
		return &dsc.Permission{}
	}
	return i.Permission
}

func (i *Permission) Hash() (string, error) {
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
