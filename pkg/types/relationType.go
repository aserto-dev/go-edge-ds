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
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	av2 "github.com/aserto-dev/go-grpc/aserto/api/v2"
	"github.com/aserto-dev/go-utils/cerr"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RelationType struct {
	*dsc.RelationType
}

func NewRelationType(i *dsc.RelationType) *RelationType {
	return &RelationType{
		RelationType: i,
	}
}

func (i *RelationType) Validate() (bool, error) {
	if i.RelationType == nil {
		return false, errors.Errorf("relation_type not instantiated")
	}
	if !(i.GetId() > 0) {
		return false, errors.Errorf("relation type id must be larger than zero")
	}
	if strings.TrimSpace(i.GetName()) == "" {
		return false, errors.Errorf("name cannot be empty")
	}
	if strings.TrimSpace(i.GetObjectType()) == "" {
		return false, errors.Errorf("object_type cannot be empty")
	}
	if !(i.GetOrdinal() >= 0) {
		return false, errors.Errorf("ordinal must be larger or equal than zero")
	}
	if !Status(i.GetStatus()).Validate() {
		return false, errors.Errorf("illegal status flag value")
	}
	return true, nil
}

func (i *RelationType) Normalize() error {
	i.Name = strings.ToLower(i.GetName())
	i.ObjectType = strings.ToLower(i.GetObjectType())
	return nil
}

func (i *RelationType) Key() string {
	return i.ObjectType + ":" + i.Name
}

func GetRelationType(ctx context.Context, i *dsc.RelationTypeIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (*RelationType, error) {
	var relTypeID int32
	if i.GetId() > 0 {
		relTypeID = i.GetId()
	} else if i.GetName() != "" && i.GetObjectType() != "" {
		key := i.GetObjectType() + ":" + i.GetName()

		idBuf, err := store.Read(RelationTypesNamePath(), key, opts)
		if err != nil {
			return nil, boltdb.ErrKeyNotFound
		}
		relTypeID = StrToInt32(string(idBuf))
	} else {
		return nil, cerr.ErrInvalidArgument
	}

	buf, err := store.Read(RelationTypesPath(), Int32ToStr(relTypeID), opts)
	if err != nil {
		return nil, err
	}

	var relType dsc.RelationType
	if err := pb.BufToProto(bytes.NewReader(buf), &relType); err != nil {
		return nil, err
	}

	return &RelationType{
		RelationType: &relType,
	}, nil
}

func GetRelationTypes(ctx context.Context, req *dsr.GetRelationTypesRequest, store *boltdb.BoltDB, opts ...boltdb.Opts) ([]*RelationType, *av2.PaginationResponse, error) {
	// filter by object type
	var objType *ObjectType
	if ok, _ := ObjectTypeIdentifier.Validate(req.Param); ok {
		var err error
		objType, err = GetObjectType(ctx, req.Param, store, opts...)
		if err != nil {
			return nil, &av2.PaginationResponse{}, err
		}
	}

	_, values, nextToken, _, err := store.List(RelationTypesPath(), req.Page.Token, req.Page.Size, opts)
	if err != nil {
		return nil, &av2.PaginationResponse{}, err
	}

	relTypes := []*RelationType{}
	for i := 0; i < len(values); i++ {
		var relType dsc.RelationType
		if err := pb.BufToProto(bytes.NewReader(values[i]), &relType); err != nil {
			return nil, &av2.PaginationResponse{}, err
		}
		if objType != nil && !strings.EqualFold(objType.Name, relType.ObjectType) {
			continue
		}
		relTypes = append(relTypes, &RelationType{&relType})
	}

	if err != nil {
		return nil, &av2.PaginationResponse{}, err
	}

	return relTypes, &av2.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(relTypes))}, nil
}

func (i *RelationType) Set(ctx context.Context, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	sessionID := session.ExtractSessionID(ctx)

	if ok, err := i.Validate(); !ok {
		return err
	}
	if err := i.Normalize(); err != nil {
		return err
	}

	curHash := ""
	current, err := GetRelationType(ctx, &dsc.RelationTypeIdentifier{
		Name:       &i.Name,
		ObjectType: &i.ObjectType,
	}, store, opts...)
	if err == nil {
		curHash = current.RelationType.Hash
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		i.RelationType.Hash = curHash
	}

	if curHash != "" && curHash != i.RelationType.Hash {
		return derr.ErrHashMismatch.Str("current", curHash).Str("incoming", i.RelationType.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		i.RelationType.CreatedAt = ts
	}
	i.RelationType.UpdatedAt = ts

	newHash, _ := i.Hash()
	i.RelationType.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		return nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, i.RelationType); err != nil {
		return err
	}

	if err := store.Write(RelationTypesPath(), Int32ToStr(i.GetId()), buf.Bytes(), opts); err != nil {
		return err
	}

	if err := store.Write(RelationTypesNamePath(), i.Key(), []byte(Int32ToStr(i.GetId())), opts); err != nil {
		return err
	}

	return nil
}

func DeleteRelationType(ctx context.Context, i *dsc.RelationTypeIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	if ok, err := RelationTypeIdentifier.Validate(i); !ok {
		return err
	}

	current, err := GetRelationType(ctx, i, store, opts...)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	key := current.ObjectType + ":" + current.Name
	if err := store.DeleteKey(RelationTypesNamePath(), key, opts); err != nil {
		return err
	}

	if err := store.DeleteKey(RelationTypesPath(), Int32ToStr(current.Id), opts); err != nil {
		return err
	}

	return nil
}

func (i *RelationType) Msg() *dsc.RelationType {
	if i == nil || i.RelationType == nil {
		return &dsc.RelationType{}
	}
	return i.RelationType
}

func (i *RelationType) Hash() (string, error) {
	h := fnv.New64a()
	h.Reset()

	if _, err := h.Write(Int32ToByte(i.GetId())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write([]byte(i.GetObjectType())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write([]byte(i.GetName())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write(Int32ToByte(i.GetOrdinal())); err != nil {
		return DefaultHash, err
	}
	if _, err := h.Write(Uint32ToByte(i.GetStatus())); err != nil {
		return DefaultHash, err
	}

	for _, u := range i.Unions {
		if _, err := h.Write([]byte(u)); err != nil {
			return DefaultHash, err
		}
	}

	for _, p := range i.Permissions {
		if _, err := h.Write([]byte(p)); err != nil {
			return DefaultHash, err
		}
	}

	return strconv.FormatUint(h.Sum64(), 10), nil
}

func GetRelationTypeID(ctx context.Context, i *dsc.RelationTypeIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (int32, error) {
	var relTypeID int32
	if i.GetId() > 0 {
		relTypeID = i.GetId()
	} else if i.GetName() != "" && i.GetObjectType() != "" {
		key := i.GetObjectType() + ":" + i.GetName()
		idBuf, err := store.Read(RelationTypesNamePath(), key, opts)
		if err != nil {
			return 0, boltdb.ErrKeyNotFound
		}
		relTypeID = StrToInt32(string(idBuf))
	} else {
		return 0, cerr.ErrInvalidArgument
	}
	return relTypeID, nil
}
