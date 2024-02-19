package ds

import (
	"github.com/aserto-dev/azm/safe"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
)

type object struct {
	*safe.SafeObject
}

func Object(i *dsc3.Object) *object { return &object{safe.Object(i)} }

func (i *object) Key() string {
	return i.GetType() + TypeIDSeparator + i.GetId()
}

type objectIdentifier struct {
	*safe.SafeObjectIdentifier
}

func ObjectIdentifier(i *dsc3.ObjectIdentifier) *objectIdentifier {
	return &objectIdentifier{safe.ObjectIdentifier(i)}
}

func (i *objectIdentifier) Key() string {
	return i.GetObjectType() + TypeIDSeparator + i.GetObjectId()
}

func ObjectSelector(i *dsc3.ObjectIdentifier) *safe.SafeObjectSelector {
	return safe.ObjectSelector(i)
}
