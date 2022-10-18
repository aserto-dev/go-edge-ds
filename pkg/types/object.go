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
	"github.com/mitchellh/hashstructure/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pkg/errors"
)

type Object struct {
	*dsc.Object
}

type Objects []*Object

func (o *Objects) Msg() []*dsc.Object {
	return []*dsc.Object{}
}

func NewObject(i *dsc.Object) *Object {
	return &Object{
		Object: i,
	}
}

func (i *Object) Validate() (bool, error) {
	if i.Object == nil {
		return false, errors.Errorf("object not instantiated")
	}
	if !ID.IsValidIfSet(i.GetId()) {
		return false, errors.Errorf("invalid object id")
	}
	if strings.TrimSpace(i.GetKey()) == "" {
		return false, derr.ErrInvalidArgument.Msg("object key cannot be empty")
	}
	if strings.TrimSpace(i.GetType()) == "" {
		return false, derr.ErrInvalidArgument.Msg("object type cannot be empty")
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

func GetObjectID(ctx context.Context, i *dsc.ObjectIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (string, error) {
	var objID string
	if ID.IsValid(i.GetId()) {
		objID = i.GetId()
	} else if i.GetKey() != "" && i.GetType() != "" {
		key := i.GetType() + "|" + i.GetKey()
		idBuf, err := store.Read(ObjectsKeyPath(), key, opts)
		if err != nil {
			return "", boltdb.ErrKeyNotFound
		}
		objID = string(idBuf)
	} else {
		return "", derr.ErrInvalidArgument
	}
	return objID, nil
}

func GetObject(ctx context.Context, i *dsc.ObjectIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (*Object, error) {
	objID, err := GetObjectID(ctx, i, store, opts...)
	if err != nil {
		return nil, err
	}

	buf, err := store.Read(ObjectsPath(), objID, opts)
	if err != nil {
		return nil, err
	}

	var obj dsc.Object
	if err := pb.BufToProto(bytes.NewReader(buf), &obj); err != nil {
		return nil, err
	}

	return &Object{
		Object: &obj,
	}, nil
}

func GetObjectMany(ctx context.Context, identifiers []*dsc.ObjectIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (Objects, error) {
	objects := []*Object{}
	for i := 0; i < len(identifiers); i++ {
		obj, err := GetObject(ctx, identifiers[i], store, opts...)
		if err != nil {
			return []*Object{}, err
		}
		objects = append(objects, obj)
	}
	return objects, nil
}

func GetObjects(ctx context.Context, page *dsc.PaginationRequest, store *boltdb.BoltDB, opts ...boltdb.Opts) (Objects, *dsc.PaginationResponse, error) {
	_, values, nextToken, _, err := store.List(ObjectsPath(), page.Token, page.Size, opts)
	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
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
		return nil, &dsc.PaginationResponse{}, err
	}

	return objects, &dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(objects))}, nil
}

func (i *Object) Set(ctx context.Context, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	sessionID := session.ExtractSessionID(ctx)

	if ok, err := i.Validate(); !ok {
		return err
	}
	if err := i.Normalize(); err != nil {
		return err
	}

	curHash := ""
	current, err := GetObject(ctx, &dsc.ObjectIdentifier{Key: &i.Key, Type: &i.Type}, store, opts...)
	if err == nil {
		curHash = current.Object.Hash
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		i.Object.Hash = curHash
	}

	if curHash != "" && curHash != i.Object.Hash {
		return derr.ErrHashMismatch.Str("current", curHash).Str("incoming", i.Object.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		i.Object.CreatedAt = ts
	}
	i.Object.UpdatedAt = ts

	newHash, _ := i.Hash()
	i.Object.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		i.Object.CreatedAt = current.CreatedAt
		i.Object.UpdatedAt = current.UpdatedAt
		return nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, i.Object); err != nil {
		return err
	}

	if err := store.Write(ObjectsPath(), i.GetId(), buf.Bytes(), opts); err != nil {
		return err
	}

	key := i.Type + "|" + i.Key
	if err := store.Write(ObjectsKeyPath(), key, []byte(i.GetId()), opts); err != nil {
		return err
	}

	return nil
}

func DeleteObject(ctx context.Context, i *dsc.ObjectIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	if ok, err := ObjectIdentifier.Validate(i); !ok {
		return err
	}

	current, err := GetObject(ctx, i, store, opts...)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	key := current.Type + "|" + current.Key
	if err := store.DeleteKey(ObjectsKeyPath(), key, opts); err != nil {
		return err
	}

	if err := store.DeleteKey(ObjectsPath(), current.Id, opts); err != nil {
		return err
	}

	return nil
}

func (i *Object) Msg() *dsc.Object {
	if i == nil || i.Object == nil {
		return &dsc.Object{}
	}
	return i.Object
}

func (i *Object) Hash() (string, error) {
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

	if _, err := h.Write([]byte(i.GetId())); err != nil {
		return DefaultHash, err
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
