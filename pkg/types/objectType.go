package types

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ObjectType struct {
	*dsc.ObjectType
}

func NewObjectType(i *dsc.ObjectType) *ObjectType {
	return &ObjectType{
		ObjectType: i,
	}
}

func (i *ObjectType) Validate() (bool, error) {
	if i.ObjectType == nil {
		return false, errors.Errorf("object_type not instantiated")
	}
	if !(i.GetId() > 0) {
		return false, errors.Errorf("object type id must be larger than zero")
	}
	if strings.TrimSpace(i.GetName()) == "" {
		return false, errors.Errorf("name cannot be empty")
	}
	if !(i.GetOrdinal() >= 0) {
		return false, errors.Errorf("ordinal must be larger or equal than zero")
	}
	if !Status(i.GetStatus()).Validate() {
		return false, errors.Errorf("illegal status flag value")
	}
	return true, nil
}

func (i *ObjectType) Normalize() error {
	i.Name = strings.ToLower(i.GetName())
	return nil
}

func GetObjectType(ctx context.Context, i *dsc.ObjectTypeIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (*ObjectType, error) {
	var name string
	if i.GetName() != "" {
		name = i.GetName()
	} else if i.GetId() > 0 {
		idBuf, err := store.Read(ObjectTypesIDPath(), fmt.Sprintf("%d", i.GetId()), opts)
		if err != nil {
			return nil, boltdb.ErrKeyNotFound
		}
		name = string(idBuf)
	} else {
		return nil, cerr.ErrInvalidArgument
	}

	buf, err := store.Read(ObjectTypesPath(), name, opts)
	if err != nil {
		return nil, err
	}

	var objType dsc.ObjectType
	if err := pb.BufToProto(bytes.NewReader(buf), &objType); err != nil {
		return nil, err
	}

	return &ObjectType{
		ObjectType: &objType,
	}, nil
}

func (i *ObjectType) Set(ctx context.Context, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	sessionID := session.ExtractSessionID(ctx)

	if ok, err := i.Validate(); !ok {
		return err
	}
	if err := i.Normalize(); err != nil {
		return err
	}

	curHash := ""
	current, err := GetObjectType(ctx, &dsc.ObjectTypeIdentifier{Name: &i.Name}, store, opts...)
	if err == nil {
		curHash = current.ObjectType.Hash
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" && i.ObjectType.Hash == "" {
		i.ObjectType.Hash = curHash
	}

	if curHash != i.ObjectType.Hash {
		return derr.ErrHashMismatch.Str("current", curHash).Str("incoming", i.ObjectType.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		i.ObjectType.CreatedAt = ts
	}
	i.ObjectType.UpdatedAt = ts

	newHash, _ := i.Hash()
	i.ObjectType.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		return nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, i.ObjectType); err != nil {
		return err
	}

	if err := store.Write(ObjectTypesPath(), i.Name, buf.Bytes(), opts); err != nil {
		return err
	}
	if err := store.Write(ObjectTypesIDPath(), fmt.Sprintf("%d", i.Id), []byte(i.Name), opts); err != nil {
		return err
	}

	return nil
}

func DeleteObjectType(ctx context.Context, i *dsc.ObjectTypeIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	if ok, err := ObjectTypeIdentifier.Validate(i); !ok {
		return err
	}

	current, err := GetObjectType(ctx, i, store, opts...)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := store.DeleteKey(ObjectTypesIDPath(), fmt.Sprintf("%d", current.Id), opts); err != nil {
		return err
	}

	if err := store.DeleteKey(ObjectTypesPath(), current.Name, opts); err != nil {
		return err
	}

	return nil
}

func (i *ObjectType) Msg() *dsc.ObjectType {
	if i == nil || i.ObjectType == nil {
		return &dsc.ObjectType{}
	}
	return i.ObjectType
}

func (i *ObjectType) Hash() (string, error) {
	h := fnv.New64a()
	h.Reset()

	if _, err := h.Write(Int32ToByte(i.GetId())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write([]byte(i.GetName())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write(BoolToByte(i.GetIsSubject())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write(Int32ToByte(i.GetOrdinal())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write(Uint32ToByte(i.GetStatus())); err != nil {
		return DefaultHash, err
	}

	return strconv.FormatUint(h.Sum64(), 10), nil
}
