package ds

import (
	"hash/fnv"
	"strconv"
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	"github.com/mitchellh/hashstructure/v2"
	"google.golang.org/protobuf/proto"
)

// model contains object related items.
const (
	objectIdentifierNil  string = "not set (nil)"
	objectIdentifierKey  string = "key"
	objectIdentifierType string = "type"
)

type object struct {
	*dsc.Object
}

func Object(i *dsc.Object) *object { return &object{i} }

func (i *object) Key() string {
	return i.GetType() + TypeIDSeparator + i.GetKey()
}

func (i *object) Validate() (bool, error) {
	if i.Object == nil {
		return false, ErrInvalidArgumentObject.Msg(objectIdentifierNil)
	}

	// #1 check is type field is set.
	if IsNotSet(i.GetType()) {
		return false, ErrInvalidArgumentObject.Msg(objectIdentifierType)
	}

	// #2 check if key field is set.
	if IsNotSet(i.GetKey()) {
		return false, ErrInvalidArgumentObjectIdentifier.Msg(objectIdentifierKey)
	}

	if i.Properties == nil {
		i.Properties = pb.NewStruct()
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
	if _, err := h.Write([]byte(i.GetKey())); err != nil {
		return DefaultHash
	}

	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash
	}

	return strconv.FormatUint(h.Sum64(), 10)
}

type objectIdentifier struct {
	*dsc.ObjectIdentifier
}

func ObjectIdentifier(i *dsc.ObjectIdentifier) *objectIdentifier { return &objectIdentifier{i} }

// TODO not used, integrated into validate or set.
func (i *objectIdentifier) Normalize() {
	i.ObjectIdentifier.Key = proto.String(strings.ToLower(strings.TrimSpace(i.GetKey())))
	i.ObjectIdentifier.Type = proto.String(strings.ToLower(strings.TrimSpace(i.GetType())))
}

func (i *objectIdentifier) Validate() (bool, error) {
	if i.ObjectIdentifier == nil {
		return false, ErrInvalidArgumentObjectIdentifier.Msg(objectIdentifierNil)
	}

	// #1 check is type field is set.
	if IsNotSet(i.GetType()) {
		return false, ErrInvalidArgumentObjectIdentifier.Msg(objectIdentifierType)
	}

	// #2 check if key field is set.
	if IsNotSet(i.GetKey()) {
		return false, ErrInvalidArgumentObjectIdentifier.Msg(objectIdentifierKey)
	}

	// #3 validate that type is defined in the type system.
	// TODO: validate type existence against TypeSystem model.

	return true, nil
}

func (i *objectIdentifier) Key() string {
	return i.GetType() + TypeIDSeparator + i.GetKey()
}

func (i *objectIdentifier) Equal(n *dsc.ObjectIdentifier) bool {
	return strings.EqualFold(i.ObjectIdentifier.GetKey(), n.GetKey()) && strings.EqualFold(i.ObjectIdentifier.GetType(), n.GetType())
}

func (i *objectIdentifier) IsComplete() bool {
	return i != nil && i.GetType() != "" && i.GetKey() != ""
}

type objectSelector struct {
	*dsc.ObjectIdentifier
}

func ObjectSelector(i *dsc.ObjectIdentifier) *objectSelector { return &objectSelector{i} }

func (i *objectSelector) Normalize() {
	i.Key = proto.String(strings.ToLower(strings.TrimSpace(i.GetKey())))
	i.Type = proto.String(strings.ToLower(strings.TrimSpace(i.GetType())))
}

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
	if IsNotSet(i.GetType()) && IsNotSet(i.GetKey()) {
		return true, nil
	}

	// type only
	if IsSet(i.GetType()) && IsNotSet(i.GetKey()) {
		return true, nil
	}

	// type + key
	if IsSet(i.GetType()) && IsSet(i.GetKey()) {
		return true, nil
	}

	return false, nil
}

func (i *objectSelector) IsComplete() bool {
	return IsSet(i.GetType()) && IsSet(i.GetKey())
}
