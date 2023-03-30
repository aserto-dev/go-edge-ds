package ds

import (
	"bytes"
	"strings"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/boltdb"
	"github.com/aserto-dev/go-edge-ds/pkg/pb"
	"google.golang.org/protobuf/proto"
)

// model contains object related items.
const (
	objectIdentifierNil  string = "not set (nil)"
	objectIdentifierKey  string = "key"
	objectIdentifierType string = "type"
)

type objectIdentifier struct {
	*dsc.ObjectIdentifier
}

func ObjectIdentifier(i *dsc.ObjectIdentifier) *objectIdentifier { return &objectIdentifier{i} }

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

func (i *objectIdentifier) Get(store *boltdb.BoltDB, opts []boltdb.Opts) (*dsc.Object, error) {
	if ok, err := i.Validate(); !ok {
		return nil, err
	}

	buf, err := store.Read(ObjectsPath, i.Key(), opts)
	if err != nil {
		return nil, err
	}

	var obj dsc.Object
	if err := pb.BufToProto(bytes.NewReader(buf), &obj); err != nil {
		return nil, err
	}

	return &obj, nil
}

func (i *objectIdentifier) Key() string {
	return i.GetType() + ":" + i.GetKey()
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
