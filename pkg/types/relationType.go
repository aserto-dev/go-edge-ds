package types

import (
	"bytes"
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
	if !(i.GetId() >= 0) {
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

func (i *RelationType) Msg() *dsc.RelationType {
	if i == nil || i.RelationType == nil {
		return &dsc.RelationType{}
	}
	return i.RelationType
}

func (i *RelationType) GetHash() (string, error) {
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

func (i *RelationType) Key() string {
	return i.ObjectType + ":" + i.Name
}

func (sc *StoreContext) GetRelationType(relTypeIdentifier *RelationTypeIdentifier) (*RelationType, error) {
	var relTypeID int32

	if relTypeIdentifier.GetId() > 0 {
		relTypeID = relTypeIdentifier.GetId()
	} else if relTypeIdentifier.GetName() != "" && relTypeIdentifier.GetObjectType() != "" {
		var err error
		relTypeID, err = sc.GetRelationTypeID(relTypeIdentifier)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, derr.ErrInvalidArgument
	}

	buf, err := sc.Store.Read(RelationTypesPath(), Int32ToStr(relTypeID), sc.Opts)
	if err != nil {
		return nil, err
	}

	var relType dsc.RelationType
	if err := pb.BufToProto(bytes.NewReader(buf), &relType); err != nil {
		return nil, err
	}

	return &RelationType{&relType}, nil
}

func (sc *StoreContext) GetRelationTypeID(relTypeIdentifier *RelationTypeIdentifier) (int32, error) {
	if relTypeIdentifier.GetId() > 0 {
		return relTypeIdentifier.GetId(), nil
	}

	idBuf, err := sc.Store.Read(RelationTypesNamePath(), relTypeIdentifier.Key(), sc.Opts)
	if err != nil {
		return 0, err
	}
	objTypeID := StrToInt32(string(idBuf))

	return objTypeID, nil
}

func (sc *StoreContext) GetRelationTypeName(relTypeIdentifier *RelationTypeIdentifier) (string, error) {
	if relTypeIdentifier.GetName() != "" && relTypeIdentifier.GetObjectType() != "" {
		return relTypeIdentifier.Key(), nil
	}

	buf, err := sc.Store.Read(RelationTypesPath(), Int32ToStr(relTypeIdentifier.GetId()), sc.Opts)
	if err != nil {
		return "", err
	}

	relType := RelationType{}
	if err := pb.BufToProto(bytes.NewReader(buf), &relType); err != nil {
		return "", err
	}

	return relType.Key(), nil
}

func (sc *StoreContext) GetRelationTypes(param *ObjectTypeIdentifier, page *PaginationRequest) ([]*RelationType, *PaginationResponse, error) {
	_, values, nextToken, _, err := sc.Store.List(RelationTypesPath(), page.Token, page.Size, sc.Opts)
	if err != nil {
		return nil, &PaginationResponse{}, err
	}

	var objType *ObjectType
	if ot, err := sc.GetObjectType(param); err == nil && ot != nil {
		objType = ot
	}

	relTypes := []*RelationType{}
	for i := 0; i < len(values); i++ {
		var relType dsc.RelationType
		if err := pb.BufToProto(bytes.NewReader(values[i]), &relType); err != nil {
			return nil, &PaginationResponse{}, err
		}
		if objType != nil && !strings.EqualFold(objType.GetName(), relType.GetObjectType()) {
			continue
		}
		relTypes = append(relTypes, &RelationType{&relType})
	}

	if err != nil {
		return nil, &PaginationResponse{}, err
	}

	return relTypes, &PaginationResponse{&dsc.PaginationResponse{NextToken: nextToken, ResultSize: int32(len(relTypes))}}, nil
}

func (sc *StoreContext) SetRelationType(relType *RelationType) (*RelationType, error) {
	sessionID := session.ExtractSessionID(sc.Context)

	if ok, err := relType.Validate(); !ok {
		return &RelationType{}, err
	}

	if err := relType.Normalize(); err != nil {
		return &RelationType{}, err
	}

	curHash := ""
	current, err := sc.GetRelationType(&RelationTypeIdentifier{
		&dsc.RelationTypeIdentifier{
			Name:       &relType.Name,
			ObjectType: &relType.ObjectType,
		},
	})
	if err == nil {
		curHash = current.Hash
		if relType.Id == 0 {
			relType.Id = current.Id
		}
	} else if relType.Id == 0 {
		if id, err := sc.Store.NextSeq(RelationTypesPath(), sc.Opts); err == nil {
			relType.Id = int32(id)
		}
	}

	// if in streaming mode, adopt current object hash, if not provided
	if sessionID != "" {
		relType.Hash = curHash
	}

	if curHash != "" && curHash != relType.Hash {
		return &RelationType{}, derr.ErrHashMismatch.Str("current", curHash).Str("incoming", relType.Hash)
	}

	ts := timestamppb.New(time.Now().UTC())
	if curHash == "" {
		relType.CreatedAt = ts
	}
	relType.UpdatedAt = ts

	newHash, _ := relType.GetHash()
	relType.Hash = newHash

	// when equal, no changes, skip write
	if curHash == newHash {
		return relType, nil
	}

	buf := new(bytes.Buffer)

	if err := pb.ProtoToBuf(buf, relType); err != nil {
		return &RelationType{}, err
	}

	if err := sc.Store.Write(RelationTypesPath(), Int32ToStr(relType.GetId()), buf.Bytes(), sc.Opts); err != nil {
		return &RelationType{}, err
	}

	if err := sc.Store.Write(RelationTypesNamePath(), relType.Key(), []byte(Int32ToStr(relType.GetId())), sc.Opts); err != nil {
		return &RelationType{}, err
	}

	return relType, nil
}

func (sc *StoreContext) DeleteRelationType(relTypeIdentifier *RelationTypeIdentifier) error {
	if ok, err := relTypeIdentifier.Validate(); !ok {
		return err
	}

	current, err := sc.GetRelationType(relTypeIdentifier)
	switch {
	case errors.Is(err, boltdb.ErrKeyNotFound):
		return nil
	case err != nil:
		return err
	}

	if err := sc.Store.DeleteKey(RelationTypesNamePath(), current.Key(), sc.Opts); err != nil {
		return err
	}

	if err := sc.Store.DeleteKey(RelationTypesPath(), Int32ToStr(current.Id), sc.Opts); err != nil {
		return err
	}

	return nil
}
