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
	"github.com/mitchellh/hashstructure/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pkg/errors"
)

type Object struct {
	*dsc.Object
}

func NewObject(i *dsc.Object) *Object {
	if i == nil {
		return &Object{Object: &dsc.Object{}}
	}
	return &Object{Object: i}
}

type Objects []*Object

func (o *Objects) Msg() []*dsc.Object {
	return []*dsc.Object{}
}

func (i *Object) PreValidate() (bool, error) {
	if i.Object == nil {
		return false, derr.ErrInvalidObject
	}
	if strings.TrimSpace(i.GetKey()) == "" {
		return false, derr.ErrInvalidArgument.Msg("object key not set")
	}
	if strings.TrimSpace(i.GetType()) == "" {
		return false, derr.ErrInvalidArgument.Msg("object type not set")
	}
	return true, nil
}

func (i *Object) Validate() (bool, error) {
	if ok, err := i.PreValidate(); !ok {
		return ok, err
	}
	return true, nil
}

func (i *Object) Normalize() error {
	i.Type = strings.ToLower(i.GetType())

	if i.Properties == nil {
		i.Properties = pb.NewStruct()
	}

	return nil
}

func (i *Object) Msg() *dsc.Object {
	if i == nil || i.Object == nil {
		return &dsc.Object{}
	}
	return i.Object
}

func (i *Object) GetHash() (string, error) {
	h := fnv.New64a()
	h.Reset()

	if i.Properties != nil {
		v := i.Properties.AsMap()
		hash, err := hashstructure.Hash(
			v,
			hashstructure.FormatV2,
			&hashstructure.HashOptions{
				Hasher: h,
			},
		)
		if err != nil {
			return DefaultHash, err
		}
		_ = hash
	}

	if _, err := h.Write([]byte(i.GetType())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write([]byte(i.GetKey())); err != nil {
		return DefaultHash, err
	}

	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash, err
	}

	return strconv.FormatUint(h.Sum64(), 10), nil
}

func (i *Object) String() string {
	return i.GetType() + "|" + i.GetKey()
}

func (sc *StoreContext) GetObject(objIdentifier *ObjectIdentifier) (*Object, error) {
	objID, err := sc.GetObjectID(objIdentifier)
	if err != nil {
		return nil, err
	}

	buf, err := sc.Store.Read(ObjectsPath(), objID, sc.Opts)
	if err != nil {
		return nil, err
	}

	var obj dsc.Object
	if err := pb.BufToProto(bytes.NewReader(buf), &obj); err != nil {
		return nil, err
	}

	return &Object{Object: &obj}, nil
}

func (sc *StoreContext) GetObjectID(objIdentifier *ObjectIdentifier) (string, error) {
	// TODO: validate
	// if ID.IsValid(objIdentifier.GetId()) {
	// 	return objIdentifier.GetId(), nil
	// }

	if objIdentifier.GetKey() != "" && objIdentifier.GetType() != "" {
		idBuf, err := sc.Store.Read(ObjectsKeyPath(), objIdentifier.Key(), sc.Opts)
		if err != nil {
			return "", boltdb.ErrKeyNotFound
		}
		return string(idBuf), nil
	}

	return "", derr.ErrInvalidArgument
}

func (sc *StoreContext) GetObjectMany(objIdentifiers []*ObjectIdentifier) (Objects, error) {
	objects := []*Object{}

	for i := 0; i < len(objIdentifiers); i++ {
		obj, err := sc.GetObject(objIdentifiers[i])
		if err != nil {
			return []*Object{}, err
		}
		objects = append(objects, obj)
	}
	return objects, nil
}

func (sc *StoreContext) GetObjects(param *ObjectTypeIdentifier, page *PaginationRequest) (Objects, *PaginationResponse, error) {
	_, values, nextToken, _, err := sc.Store.List(ObjectsPath(), page.Token, page.Size, sc.Opts)
	if err != nil {
		return nil, &PaginationResponse{}, err
	}

	objects := []*Object{}
	for i := 0; i < len(values); i++ {
		var object dsc.Object
		if err := pb.BufToProto(bytes.NewReader(values[i]), &object); err != nil {
			return nil, nil, err
		}
		objects = append(objects, &Object{&object})
	}

	if err != nil {
		return nil, &PaginationResponse{}, err
	}

	return objects, &PaginationResponse{&dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(objects))}}, nil
}

func (sc *StoreContext) SetObject(obj *Object) (*Object, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	if ok, err := obj.PreValidate(); !ok {
		return &Object{}, err
	}

	curHash := ""
	current, err := sc.GetObject(&ObjectIdentifier{
		&dsc.ObjectIdentifier{
			Key: &obj.Key, Type: &obj.Type,
		},
	})
	if err == nil {
		curHash = current.Object.Hash
	}

	if ok, err := obj.Validate(); !ok {
		return &Object{}, err
	}

	if err := obj.Normalize(); err != nil {
		return &Object{}, err
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		obj.Hash = curHash
	}

	if curHash != "" && curHash != obj.Hash {
		return &Object{}, derr.ErrHashMismatch.Str("current", curHash).Str("incoming", obj.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		obj.CreatedAt = ts
	}
	obj.UpdatedAt = ts

	newHash, _ := obj.GetHash()
	obj.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		obj.CreatedAt = current.CreatedAt
		obj.UpdatedAt = current.UpdatedAt
		return obj, nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, obj); err != nil {
		return &Object{}, err
	}

	key := obj.String()
	if err := sc.Store.Write(ObjectsPath(), key, buf.Bytes(), sc.Opts); err != nil {
		return &Object{}, err
	}

	return obj, nil
}

func (sc *StoreContext) DeleteObject(objIdentifier *ObjectIdentifier) error {
	if ok, err := objIdentifier.Validate(); !ok {
		return err
	}

	current, err := sc.GetObject(objIdentifier)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	key := current.String()
	if err := sc.Store.DeleteKey(ObjectsPath(), key, sc.Opts); err != nil {
		return err
	}

	return nil
}
