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
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-lib/ids"
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

func GetRelationType(ctx context.Context, i *dsc.RelationTypeIdentifier, store *boltdb.BoltDB, opts ...boltdb.Opts) (*RelationType, error) {
	var (
		relTypeName string
		objTypeName string
	)

	if i.GetName() != "" && i.GetObjectType() != "" {
		relTypeName = i.GetName()
		objTypeName = i.GetObjectType()
	} else if i.GetId() > 0 {
		idBuf, err := store.Read(RelationTypesIDPath(), fmt.Sprintf("%d", i.GetId()), opts)
		if err != nil {
			return nil, boltdb.ErrKeyNotFound
		}
		s := strings.Split(string(idBuf), "|")
		objTypeName, relTypeName = s[0], s[1]
	} else {
		return nil, cerr.ErrInvalidArgument
	}

	key := objTypeName + "|" + relTypeName
	buf, err := store.Read(RelationTypesPath(), key, opts)
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

func (i *RelationType) Set(ctx context.Context, store *boltdb.BoltDB, opts ...boltdb.Opts) error {
	sessionID := ids.ExtractSessionID(ctx)

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
	if sessionID != "" && i.RelationType.Hash == "" {
		i.RelationType.Hash = curHash
	}

	if curHash != i.RelationType.Hash {
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

	key := i.ObjectType + "|" + i.Name
	if err := store.Write(RelationTypesPath(), key, buf.Bytes(), opts); err != nil {
		return err
	}
	if err := store.Write(RelationTypesIDPath(), fmt.Sprintf("%d", i.Id), []byte(key), opts); err != nil {
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

	if err := store.DeleteKey(RelationTypesIDPath(), fmt.Sprintf("%d", current.Id), opts); err != nil {
		return err
	}

	key := current.ObjectType + "|" + current.Name
	if err := store.DeleteKey(RelationTypesPath(), key, opts); err != nil {
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
