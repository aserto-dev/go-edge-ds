package ds

import (
	"bytes"
	"context"
	"hash/fnv"
	"strconv"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	bolt "go.etcd.io/bbolt"
)

// Permission.
type permission struct {
	*dsc.Permission
}

func Permission(i *dsc.Permission) *permission { return &permission{i} }

func (i *permission) Key() string {
	return i.GetName()
}

func (i *permission) Validate() (bool, error) {
	if i.Permission == nil {
		return false, ErrInvalidArgumentPermission.Msg("not set (nil)")
	}

	if IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentPermission.Msg("name")
	}

	return true, nil
}

func (i *permission) Hash() string {
	h := fnv.New64a()
	h.Reset()

	if _, err := h.Write([]byte(i.GetName())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash
	}

	return strconv.FormatUint(h.Sum64(), 10)
}

// PermissionIdentifier.
type permissionIdentifier struct {
	*dsc.PermissionIdentifier
}

func PermissionIdentifier(i *dsc.PermissionIdentifier) *permissionIdentifier {
	return &permissionIdentifier{i}
}

func (i *permissionIdentifier) Key() string {
	return i.GetName()
}

func (i *permissionIdentifier) Validate() (bool, error) {
	if i.PermissionIdentifier == nil {
		return false, ErrInvalidArgumentPermissionIdentifier.Msg("not set (nil)")
	}

	if IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentPermissionIdentifier.Msg("name")
	}

	return true, nil
}

func (i *permissionIdentifier) Get(ctx context.Context, db *bolt.DB, tx *bolt.Tx) (*dsc.Permission, error) {
	if ok, err := i.Validate(); !ok {
		return nil, err
	}

	buf, err := boltdb.GetKey(tx, PermissionsPath, i.Key())
	if err != nil {
		return nil, err
	}

	var perm dsc.Permission
	if err := pb.BufToProto(bytes.NewReader(buf), &perm); err != nil {
		return nil, err
	}

	return &perm, nil
}
