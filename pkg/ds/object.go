package ds

import (
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/model"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	"github.com/aserto-dev/go-directory/pkg/derr"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"

	"github.com/mitchellh/hashstructure/v2"
)

// model contains object related items.
const (
	objectIdentifierNil  string = "not set (nil)"
	objectIdentifierKey  string = "key"
	objectIdentifierType string = "type"
)

type object struct {
	*dsc3.Object
}

func Object(i *dsc3.Object) *object { return &object{i} }

func (i *object) Key() string {
	return i.GetType() + TypeIDSeparator + i.GetId()
}

func (i *object) Validate(mc *cache.Cache) (bool, error) {
	if i.Object == nil {
		return false, ErrInvalidArgumentObject.Msg(objectIdentifierNil)
	}

	// #1 check is type field is set.
	if IsNotSet(i.GetType()) {
		return false, ErrInvalidArgumentObject.Msg(objectIdentifierType)
	}

	// #2 check if id field is set.
	if IsNotSet(i.GetId()) {
		return false, ErrInvalidArgumentObjectIdentifier.Msg(objectIdentifierKey)
	}

	if i.Properties == nil {
		i.Properties = pb.NewStruct()
	}

	if mc == nil {
		return true, nil
	}

	if !mc.ObjectExists(model.ObjectName(i.Object.Type)) {
		return false, derr.ErrObjectTypeNotFound.Msg(i.Object.Type)
	}

	return true, nil
}

func (i *object) Hash() string {
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
			return DefaultHash
		}
		_ = hash
	}

	if _, err := h.Write([]byte(i.GetType())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write([]byte(i.GetId())); err != nil {
		return DefaultHash
	}

	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash
	}

	return strconv.FormatUint(h.Sum64(), 10)
}

type objectIdentifier struct {
	*dsc3.ObjectIdentifier
}

func ObjectIdentifier(i *dsc3.ObjectIdentifier) *objectIdentifier { return &objectIdentifier{i} }

// TODO not used, integrated into validate or set.
// func (i *objectIdentifier) Normalize() {
// 	i.ObjectIdentifier.Type = proto.String(strings.ToLower(strings.TrimSpace(i.GetType())))
// 	i.ObjectIdentifier.Key = proto.String(strings.ToLower(strings.TrimSpace(i.GetKey())))
// }

func (i *objectIdentifier) Validate() (bool, error) {
	if i.ObjectIdentifier == nil {
		return false, ErrInvalidArgumentObjectIdentifier.Msg(objectIdentifierNil)
	}

	// #1 check is type field is set.
	if IsNotSet(i.GetObjectType()) {
		return false, ErrInvalidArgumentObjectIdentifier.Msg(objectIdentifierType)
	}

	// #2 check if id field is set.
	if IsNotSet(i.GetObjectId()) {
		return false, ErrInvalidArgumentObjectIdentifier.Msg(objectIdentifierKey)
	}

	// #3 validate that type is defined in the type system.
	// TODO: validate type existence against TypeSystem model.

	return true, nil
}

func (i *objectIdentifier) Key() string {
	return i.GetObjectType() + TypeIDSeparator + i.GetObjectId()
}

func (i *objectIdentifier) Equal(n *dsc3.ObjectIdentifier) bool {
	return strings.EqualFold(i.ObjectIdentifier.GetObjectId(), n.GetObjectId()) && strings.EqualFold(i.ObjectIdentifier.GetObjectType(), n.GetObjectType())
}

func (i *objectIdentifier) IsComplete() bool {
	return i != nil && i.GetObjectType() != "" && i.GetObjectId() != ""
}

type objectSelector struct {
	*dsc3.ObjectIdentifier
}

func ObjectSelector(i *dsc3.ObjectIdentifier) *objectSelector { return &objectSelector{i} }

// Validate rules:
// valid states
// - empty object
// - type only
// - type + key.
func (i *objectSelector) Validate() (bool, error) {
	// nil not allowed
	if i.ObjectIdentifier == nil {
		return false, ErrInvalidArgumentObjectTypeSelector.Msg(objectIdentifierNil)
	}

	// empty object
	if IsNotSet(i.GetObjectType()) && IsNotSet(i.GetObjectId()) {
		return true, nil
	}

	// type only
	if IsSet(i.GetObjectType()) && IsNotSet(i.GetObjectId()) {
		return true, nil
	}

	// type + key
	if IsSet(i.GetObjectType()) && IsSet(i.GetObjectId()) {
		return true, nil
	}

	return false, nil
}

func (i *objectSelector) IsComplete() bool {
	return IsSet(i.GetObjectType()) && IsSet(i.GetObjectId())
}
