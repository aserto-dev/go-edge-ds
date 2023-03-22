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

type ObjectType struct {
	*dsc.ObjectType
}

func NewObjectType(i *dsc.ObjectType) *ObjectType {
	if i == nil {
		return &ObjectType{ObjectType: &dsc.ObjectType{}}
	}
	return &ObjectType{ObjectType: i}
}

func (i *ObjectType) PreValidate() (bool, error) {
	if i == nil {
		return false, derr.ErrInvalidObjectType
	}
	if strings.TrimSpace(i.Name) == "" {
		return false, derr.ErrInvalidArgument.Msg("name not set")
	}
	if !(i.Ordinal >= 0) {
		return false, derr.ErrInvalidArgument.Msg("ordinal must be larger or equal than zero")
	}
	if !Status(i.Status).Validate() {
		return false, derr.ErrInvalidArgument.Msg("unknown status flag value")
	}
	return true, nil
}

func (i *ObjectType) Validate() (bool, error) {
	if ok, err := i.PreValidate(); !ok {
		return ok, err
	}
	return true, nil
}

func (i *ObjectType) Normalize() error {
	i.Name = strings.ToLower(i.Name)
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

func (sc *StoreContext) GetObjectType(objTypeIdentifier *ObjectTypeIdentifier) (*ObjectType, error) {
	if objTypeIdentifier.GetName() == "" {
		return nil, derr.ErrInvalidArgument.Msg("object type name not set")
	}

	buf, err := sc.Store.Read(ObjectTypesPath(), objTypeIdentifier.GetName(), sc.Opts)
	if err != nil {
		return nil, err
	}

	var objType dsc.ObjectType
	if err := pb.BufToProto(bytes.NewReader(buf), &objType); err != nil {
		return nil, err
	}

	return &ObjectType{&objType}, nil
}

func (sc *StoreContext) GetObjectTypes(page *PaginationRequest) ([]*ObjectType, *PaginationResponse, error) {
	_, values, nextToken, _, err := sc.Store.List(ObjectTypesPath(), page.Token, page.Size, sc.Opts)
	if err != nil {
		return nil, &PaginationResponse{}, err
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
		return nil, &PaginationResponse{}, err
	}

	return objTypes, &PaginationResponse{&dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(objTypes))}}, nil
}

func (sc *StoreContext) SetObjectType(objType *ObjectType) (*ObjectType, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	if ok, err := objType.PreValidate(); !ok {
		return &ObjectType{}, err
	}

	curHash := ""
	current, err := sc.GetObjectType(&ObjectTypeIdentifier{&dsc.ObjectTypeIdentifier{Name: &objType.Name}})
	if err == nil {
		curHash = current.Hash
	}

	if ok, err := objType.Validate(); !ok {
		return &ObjectType{}, err
	}

	if err := objType.Normalize(); err != nil {
		return &ObjectType{}, err
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		objType.Hash = curHash
	}

	if curHash != "" && curHash != objType.Hash {
		return &ObjectType{}, derr.ErrHashMismatch.Str("current", curHash).Str("incoming", objType.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		objType.CreatedAt = ts
	}
	objType.UpdatedAt = ts

	newHash, _ := objType.GetHash()
	objType.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		objType.CreatedAt = current.CreatedAt
		objType.UpdatedAt = current.UpdatedAt
		return objType, nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, objType); err != nil {
		return &ObjectType{}, err
	}

	if err := sc.Store.Write(ObjectTypesPath(), objType.Name, buf.Bytes(), sc.Opts); err != nil {
		return &ObjectType{}, err
	}

	return objType, nil
}

func (sc *StoreContext) DeleteObjectType(objTypeIdentifier *ObjectTypeIdentifier) error {
	if ok, err := objTypeIdentifier.Validate(); !ok {
		return err
	}

	current, err := sc.GetObjectType(objTypeIdentifier)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := sc.Store.DeleteKey(ObjectTypesPath(), current.Name, sc.Opts); err != nil {
		return err
	}

	return nil
}
