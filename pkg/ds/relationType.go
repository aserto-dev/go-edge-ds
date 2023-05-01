package ds

// model contains relation type related items.

import (
	"bytes"
	"context"
	"hash/fnv"
	"strconv"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	bolt "go.etcd.io/bbolt"
)

// RelationType.
type relationType struct {
	*dsc.RelationType
}

func RelationType(i *dsc.RelationType) *relationType { return &relationType{i} }

func (i *relationType) Key() string {
	return i.ObjectType + ":" + i.Name
}

func (i *relationType) Validate() (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentRelationType.Msg("relation type not set (nil)")
	}

	if IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentRelationType.Msg("name")
	}

	if IsNotSet(i.GetObjectType()) {
		return false, ErrInvalidArgumentRelationType.Msg("object_type")
	}

	return true, nil
}

func (i *relationType) Hash() string {
	h := fnv.New64a()
	h.Reset()

	if _, err := h.Write([]byte(i.GetObjectType())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write([]byte(i.GetName())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write(Int32ToByte(i.GetOrdinal())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write(Uint32ToByte(i.GetStatus())); err != nil {
		return DefaultHash
	}

	for _, u := range i.Unions {
		if _, err := h.Write([]byte(u)); err != nil {
			return DefaultHash
		}
	}

	for _, p := range i.Permissions {
		if _, err := h.Write([]byte(p)); err != nil {
			return DefaultHash
		}
	}

	return strconv.FormatUint(h.Sum64(), 10)
}

// RelationTypeIdentifier.
type relationTypeIdentifier struct {
	*dsc.RelationTypeIdentifier
}

func RelationTypeIdentifier(i *dsc.RelationTypeIdentifier) *relationTypeIdentifier {
	return &relationTypeIdentifier{i}
}

func (i *relationTypeIdentifier) Key() string {
	return i.GetObjectType() + ":" + i.GetName()
}

func (i *relationTypeIdentifier) Validate() (bool, error) {
	if i.RelationTypeIdentifier == nil {
		return false, ErrInvalidArgumentRelationTypeIdentifier.Msg("not set (nil)")
	}

	if IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentRelationTypeIdentifier.Msg("name")
	}

	if IsNotSet(i.GetObjectType()) {
		return false, ErrInvalidArgumentRelationTypeIdentifier.Msg("object_type")
	}

	return true, nil
}

// RelationTypeSelector.
type relationTypeSelector struct {
	*dsc.RelationTypeIdentifier
}

func RelationTypeSelector(i *dsc.RelationTypeIdentifier) *relationTypeSelector {
	return &relationTypeSelector{i}
}

func (i *relationTypeSelector) Validate() (bool, error) {
	if i.RelationTypeIdentifier == nil {
		return false, ErrInvalidArgumentRelationTypeIdentifier.Msg("not set(nil)")
	}

	// TODO : validate that if Name is set, the object type exists in type system.

	return true, nil
}

func (i *relationTypeIdentifier) Get(ctx context.Context, db *bolt.DB, tx *bolt.Tx) (*dsc.RelationType, error) {
	if ok, err := i.Validate(); !ok {
		return nil, err
	}

	buf, err := boltdb.GetKey(tx, RelationTypesPath, i.Key())
	if err != nil {
		return nil, err
	}

	var relType dsc.RelationType
	if err := pb.BufToProto(bytes.NewReader(buf), &relType); err != nil {
		return nil, err
	}

	return &relType, nil
}
