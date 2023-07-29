package ds

import (
	"hash/fnv"
	"strconv"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
)

type objectType struct {
	*dsc2.ObjectType
}

func ObjectType(i *dsc2.ObjectType) *objectType { return &objectType{i} }

func (i *objectType) Key() string {
	return i.GetName()
}

func (i *objectType) Validate() (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentObjectType.Msg("object type not set (nil)")
	}

	if IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentObjectType.Msg("name")
	}

	return true, nil
}

func (i *objectType) Hash() string {
	h := fnv.New64a()
	h.Reset()

	if _, err := h.Write([]byte(i.GetName())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write(BoolToByte(i.GetIsSubject())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write(Int32ToByte(i.GetOrdinal())); err != nil {
		return DefaultHash
	}
	if _, err := h.Write(Uint32ToByte(i.GetStatus())); err != nil {
		return DefaultHash
	}

	return strconv.FormatUint(h.Sum64(), 10)
}

type objectTypeIdentifier struct {
	*dsc2.ObjectTypeIdentifier
}

func ObjectTypeIdentifier(i *dsc2.ObjectTypeIdentifier) *objectTypeIdentifier {
	return &objectTypeIdentifier{i}
}

func (i *objectTypeIdentifier) Key() string {
	return i.GetName()
}

func (i *objectTypeIdentifier) Validate() (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentObjectTypeIdentifier.Msg(objectIdentifierNil)
	}

	if IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentObjectTypeIdentifier.Msg("name")
	}

	return true, nil
}

type objectTypeSelector struct {
	*dsc2.ObjectTypeIdentifier
}

func ObjectTypeSelector(i *dsc2.ObjectTypeIdentifier) *objectTypeSelector {
	if i == nil {
		return &objectTypeSelector{&dsc2.ObjectTypeIdentifier{}}
	}
	return &objectTypeSelector{i}
}

func (i *objectTypeSelector) Validate() (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentObjectTypeIdentifier.Msg(objectIdentifierNil)
	}

	if i.Name != nil && IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentObjectTypeIdentifier.Msg("name")
	}

	return true, nil
}
