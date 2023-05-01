package ds

import (
	"context"

	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
)

type checkPermission struct {
	*dsr.CheckPermissionRequest
}

func CheckPermission(i *dsr.CheckPermissionRequest) *checkPermission {
	return &checkPermission{i}
}

func (i *checkPermission) Validate() (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check permission request not set (nil)")
	}

	if i.CheckPermissionRequest == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check permission request not set (nil)")
	}

	if ok, err := ObjectIdentifier(i.CheckPermissionRequest.Object).Validate(); !ok {
		return ok, err
	}

	if ok, err := PermissionIdentifier(i.CheckPermissionRequest.Permission).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectIdentifier(i.CheckPermissionRequest.Subject).Validate(); !ok {
		return ok, err
	}

	return true, nil
}

func (i *checkPermission) Exec(ctx context.Context) (*dsr.CheckPermissionResponse, error) {
	return nil, nil
}

type checkRelation struct {
	*dsr.CheckRelationRequest
}

func CheckRelation(i *dsr.CheckRelationRequest) *checkRelation {
	return &checkRelation{i}
}

func (i *checkRelation) Validate() (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check relation request not set (nil)")
	}

	if i.CheckRelationRequest == nil {
		return false, ErrInvalidArgumentObjectType.Msg("check relations request not set (nil)")
	}

	if ok, err := ObjectIdentifier(i.CheckRelationRequest.Object).Validate(); !ok {
		return ok, err
	}

	if ok, err := RelationTypeIdentifier(i.CheckRelationRequest.Relation).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectIdentifier(i.CheckRelationRequest.Subject).Validate(); !ok {
		return ok, err
	}

	return true, nil
}

func (i *checkRelation) Exec(ctx context.Context) (*dsr.CheckRelationResponse, error) {
	return nil, nil
}
