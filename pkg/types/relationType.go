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

type relationType struct {
	*dsc.RelationType
}

func RelationType(i *dsc.RelationType) *relationType { return &relationType{i} }

func (i *relationType) PreValidate() (bool, error) {
	if i.RelationType == nil {
		return false, derr.ErrInvalidRelationType
	}
	if strings.TrimSpace(i.GetName()) == "" {
		return false, derr.ErrInvalidArgument.Msg("name not set")
	}
	if strings.TrimSpace(i.GetObjectType()) == "" {
		return false, derr.ErrInvalidArgument.Msg("object_type not set")
	}
	if !(i.GetOrdinal() >= 0) {
		return false, derr.ErrInvalidArgument.Msg("ordinal must be larger or equal than zero")
	}
	if !Status(i.GetStatus()).Validate() {
		return false, derr.ErrInvalidArgument.Msg("unknown status flag value")
	}
	return true, nil
}

func (i *relationType) Validate() (bool, error) {
	if ok, err := i.PreValidate(); !ok {
		return ok, err
	}
	return true, nil
}

func (i *relationType) Normalize() error {
	i.Name = strings.ToLower(i.GetName())
	i.ObjectType = strings.ToLower(i.GetObjectType())
	return nil
}

func (i *relationType) GetHash() (string, error) {
	h := fnv.New64a()
	h.Reset()

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

func (i *relationType) Key() string {
	return i.ObjectType + ":" + i.Name
}

func (sc *StoreContext) GetRelationType(relTypeIdentifier *dsc.RelationTypeIdentifier) (*dsc.RelationType, error) {

	buf, err := sc.Store.Read(RelationTypesPath(), RelationTypeIdentifier(relTypeIdentifier).Key(), sc.Opts)
	if err != nil {
		return nil, err
	}

	var relType dsc.RelationType
	if err := pb.BufToProto(bytes.NewReader(buf), &relType); err != nil {
		return nil, err
	}

	return &relType, nil
}

func (sc *StoreContext) GetRelationTypeName(relTypeIdentifier *dsc.RelationTypeIdentifier) (string, error) {
	relType, err := sc.GetRelationType(relTypeIdentifier)
	if err != nil {
		return "", err
	}
	return RelationType(relType).Key(), nil
}

func (sc *StoreContext) GetRelationTypes(param *dsc.ObjectTypeIdentifier, page *dsc.PaginationRequest) ([]*dsc.RelationType, *dsc.PaginationResponse, error) {
	_, values, nextToken, _, err := sc.Store.List(RelationTypesPath(), page.Token, page.Size, sc.Opts)
	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
	}

	var objType *dsc.ObjectType
	if ot, err := sc.GetObjectType(param); err == nil && ot != nil {
		objType = ot
	}

	relTypes := []*dsc.RelationType{}
	for i := 0; i < len(values); i++ {
		var relType dsc.RelationType
		if err := pb.BufToProto(bytes.NewReader(values[i]), &relType); err != nil {
			return nil, &dsc.PaginationResponse{}, err
		}
		if objType != nil && !strings.EqualFold(objType.GetName(), relType.GetObjectType()) {
			continue
		}
		relTypes = append(relTypes, &relType)
	}

	if err != nil {
		return nil, &dsc.PaginationResponse{}, err
	}

	return relTypes, &dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(relTypes))}, nil
}

func (sc *StoreContext) SetRelationType(relType *dsc.RelationType) (*dsc.RelationType, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	if ok, err := RelationType(relType).PreValidate(); !ok {
		return &dsc.RelationType{}, err
	}

	curHash := ""
	current, err := sc.GetRelationType(&dsc.RelationTypeIdentifier{
		Name:       &relType.Name,
		ObjectType: &relType.ObjectType,
	})
	if err == nil {
		curHash = current.Hash
	}

	if ok, err := RelationType(relType).Validate(); !ok {
		return &dsc.RelationType{}, err
	}

	if err := RelationType(relType).Normalize(); err != nil {
		return &dsc.RelationType{}, err
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		relType.Hash = curHash
	}

	if curHash != "" && curHash != relType.Hash {
		return &dsc.RelationType{}, derr.ErrHashMismatch.Str("current", curHash).Str("incoming", relType.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		relType.CreatedAt = ts
	}
	relType.UpdatedAt = ts

	newHash, _ := RelationType(relType).GetHash()
	relType.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		relType.CreatedAt = current.CreatedAt
		relType.UpdatedAt = current.UpdatedAt
		return relType, nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, relType); err != nil {
		return &dsc.RelationType{}, err
	}

	if err := sc.Store.Write(RelationTypesPath(), RelationType(relType).Key(), buf.Bytes(), sc.Opts); err != nil {
		return &dsc.RelationType{}, err
	}

	return relType, nil
}

func (sc *StoreContext) DeleteRelationType(relTypeIdentifier *dsc.RelationTypeIdentifier) error {
	if ok, err := RelationTypeIdentifier(relTypeIdentifier).Validate(); !ok {
		return err
	}

	current, err := sc.GetRelationType(relTypeIdentifier)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := sc.Store.DeleteKey(RelationTypesPath(), RelationType(current).Key(), sc.Opts); err != nil {
		return err
	}

	return nil
}
