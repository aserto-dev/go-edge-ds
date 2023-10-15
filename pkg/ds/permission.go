package ds

import (
	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
)

// Permission.
type permission struct {
	*dsc2.Permission
}

func Permission(i *dsc2.Permission) *permission { return &permission{i} }

func (i *permission) Key() string {
	return i.GetName()
}

func (i *permission) Validate() (bool, error) {
	if i.Permission == nil {
		return false, ErrInvalidArgumentPermission.Msg("not set (nil)")
	}

	if IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentPermission.Msg("name")
	}

	return true, nil
}

// func (i *permission) Hash() string {
// 	h := fnv.New64a()
// 	h.Reset()

// 	if _, err := h.Write([]byte(i.GetName())); err != nil {
// 		return DefaultHash
// 	}
// 	if _, err := h.Write([]byte(i.GetDisplayName())); err != nil {
// 		return DefaultHash
// 	}

// 	return strconv.FormatUint(h.Sum64(), 10)
// }

// PermissionIdentifier.
type permissionIdentifier struct {
	*dsc2.PermissionIdentifier
}

func PermissionIdentifier(i *dsc2.PermissionIdentifier) *permissionIdentifier {
	return &permissionIdentifier{i}
}

func (i *permissionIdentifier) Key() string {
	return i.GetName()
}

func (i *permissionIdentifier) Validate() (bool, error) {
	if i.PermissionIdentifier == nil {
		return false, ErrInvalidArgumentPermissionIdentifier.Msg("not set (nil)")
	}

	if IsNotSet(i.GetName()) {
		return false, ErrInvalidArgumentPermissionIdentifier.Msg("name")
	}

	return true, nil
}
