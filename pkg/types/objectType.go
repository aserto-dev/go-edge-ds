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
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ObjectType struct {
	*dsc.ObjectType
}

func NewObjectType(i *dsc.ObjectType) *ObjectType {
	return &ObjectType{i}
}

func (i *ObjectType) Validate() (bool, error) {
	if i == nil {
		return false, errors.Errorf("object_type not instantiated")
	}
	if !(i.Id > 0) {
		return false, errors.Errorf("object type id must be larger than zero")
	}
	if strings.TrimSpace(i.Name) == "" {
		return false, errors.Errorf("name cannot be empty")
	}
	if !(i.Ordinal >= 0) {
		return false, errors.Errorf("ordinal must be larger or equal than zero")
	}
	if !Status(i.Status).Validate() {
		return false, errors.Errorf("illegal status flag value")
	}
	return true, nil
}

func (i *ObjectType) Normalize() error {
	i.Name = strings.ToLower(i.Name)
	return nil
}

func GetObjectType(ctx context.Context, i *dsc.ObjectTypeIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (*ObjectType, error) {
	var objTypeID int32
	if i.GetId() > 0 {
		objTypeID = i.GetId()
	} else if i.GetName() != "" {
		idBuf, err := store.Read(ObjectTypesNamePath(), i.GetName(), opts)
		if err != nil {
			return nil, err
		}
		objTypeID = StrToInt32(string(idBuf))
	} else {
		return nil, derr.ErrInvalidArgument
	}

	buf, err := store.Read(ObjectTypesPath(), Int32ToStr(objTypeID), opts)
	if err != nil {
		return nil, err
	}

	var objType dsc.ObjectType
	if err := pb.BufToProto(bytes.NewReader(buf), &objType); err != nil {
		return nil, err
	}

	return &ObjectType{&objType}, nil
}

func GetObjectTypes(ctx context.Context, page *dsc.PaginationRequest, store *boltdb.BoltDB, opts ...boltdb.Opts) ([]*ObjectType, *dsc.PaginationResponse, error) {
	_, values, nextToken, _, err := store.List(ObjectTypesPath(), page.Token, page.Size, opts)
	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
	}

	objTypes := []*ObjectType{}
	for i := 0; i < len(values); i++ {
		var objType dsc.ObjectType
		if err := pb.BufToProto(bytes.NewReader(values[i]), &objType); err != nil {
			return nil, nil, err
		}
		objTypes = append(objTypes, &ObjectType{&objType})
	}

	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
	}

	return objTypes, &dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(objTypes))}, nil
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
		curHash = current.Hash
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		i.Hash = curHash
	}

	if curHash != "" && curHash != i.Hash {
		return derr.ErrHashMismatch.Str("current", curHash).Str("incoming", i.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		i.CreatedAt = ts
	}
	i.UpdatedAt = ts

	newHash, _ := i.GetHash()
	i.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		return nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, i); err != nil {
		return err
	}

	if err := store.Write(ObjectTypesPath(), Int32ToStr(i.Id), buf.Bytes(), opts); err != nil {
		return err
	}
	if err := store.Write(ObjectTypesNamePath(), i.Name, []byte(Int32ToStr(i.Id)), opts); err != nil {
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

	if err := store.DeleteKey(ObjectTypesNamePath(), current.Name, opts); err != nil {
		return err
	}

	if err := store.DeleteKey(ObjectTypesPath(), Int32ToStr(current.Id), opts); err != nil {
		return err
	}

	return nil
}

func (i *ObjectType) Msg() *dsc.ObjectType {
	if i == nil {
		return &dsc.ObjectType{}
	}
	return i.ObjectType
}

func (i *ObjectType) GetHash() (string, error) {
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
