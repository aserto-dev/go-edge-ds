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

type object struct {
	*dsc.Object
}

func Object(i *dsc.Object) *object { return &object{i} }

func (i *object) PreValidate() (bool, error) {
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

func (i *object) Validate() (bool, error) {
	if ok, err := i.PreValidate(); !ok {
		return ok, err
	}
	return true, nil
}

func (i *object) Normalize() error {
	i.Type = strings.ToLower(i.GetType())

	if i.Properties == nil {
		i.Properties = pb.NewStruct()
	}

	return nil
}

func (i *object) GetHash() (string, error) {
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

func (i *object) String() string {
	return i.GetType() + ":" + i.GetKey()
}

func (sc *StoreContext) GetObject(i *dsc.ObjectIdentifier) (*dsc.Object, error) {
	if ok, err := ObjectIdentifier(i).Validate(); !ok {
		return nil, err
	}

	buf, err := sc.Store.Read(ObjectsPath(), ObjectIdentifier(i).Key(), sc.Opts)
	if err != nil {
		return nil, err
	}

	var obj dsc.Object
	if err := pb.BufToProto(bytes.NewReader(buf), &obj); err != nil {
		return nil, err
	}

	return &obj, nil
}

func (sc *StoreContext) GetObjectMany(objIdentifiers []*dsc.ObjectIdentifier) ([]*dsc.Object, error) {
	objects := []*dsc.Object{}

	for i := 0; i < len(objIdentifiers); i++ {
		obj, err := sc.GetObject(objIdentifiers[i])
		if err != nil {
			return []*dsc.Object{}, err
		}
		objects = append(objects, obj)
	}
	return objects, nil
}

func (sc *StoreContext) GetObjects(param *dsc.ObjectTypeIdentifier, page *dsc.PaginationRequest) ([]*dsc.Object, *dsc.PaginationResponse, error) {
	// TODO: object type name filter is not implemented
	_, values, nextToken, _, err := sc.Store.List(ObjectsPath(), page.Token, page.Size, sc.Opts)
	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
	}

	objects := []*dsc.Object{}
	for i := 0; i < len(values); i++ {
		var object dsc.Object
		if err := pb.BufToProto(bytes.NewReader(values[i]), &object); err != nil {
			return nil, nil, err
		}
		objects = append(objects, &object)
	}

	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
	}

	return objects, &dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(objects))}, nil
}

func (sc *StoreContext) SetObject(obj *dsc.Object) (*dsc.Object, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	if ok, err := Object(obj).PreValidate(); !ok {
		return &dsc.Object{}, err
	}

	curHash := ""
	current, err := sc.GetObject(&dsc.ObjectIdentifier{
		Type: &obj.Type,
		Key:  &obj.Key,
	})
	if err == nil {
		curHash = current.Hash
	}

	if ok, err := Object(obj).Validate(); !ok {
		return &dsc.Object{}, err
	}

	if err := Object(obj).Normalize(); err != nil {
		return &dsc.Object{}, err
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		obj.Hash = curHash
	}

	if curHash != "" && curHash != obj.Hash {
		return &dsc.Object{}, derr.ErrHashMismatch.Msgf("cur: %s new: %s", curHash, obj.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		obj.CreatedAt = ts
	}
	obj.UpdatedAt = ts

	newHash, _ := Object(obj).GetHash()
	obj.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		obj.CreatedAt = current.CreatedAt
		obj.UpdatedAt = current.UpdatedAt
		return obj, nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, obj); err != nil {
		return &dsc.Object{}, err
	}

	key := obj.String()
	if err := sc.Store.Write(ObjectsPath(), key, buf.Bytes(), sc.Opts); err != nil {
		return &dsc.Object{}, err
	}

	return obj, nil
}

func (sc *StoreContext) DeleteObject(i *dsc.ObjectIdentifier) error {
	if ok, err := ObjectIdentifier(i).Validate(); !ok {
		return err
	}

	current, err := sc.GetObject(i)
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
