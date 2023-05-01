package ds

import (
	"context"

	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
)

type getGraph struct {
	*dsr.GetGraphRequest
}

func GetGraph(i *dsr.GetGraphRequest) *getGraph {
	return &getGraph{i}
}

func (i *getGraph) Validate() (bool, error) {
	if i == nil {
		return false, ErrInvalidArgumentObjectType.Msg("get graph request not set (nil)")
	}

	if i.GetGraphRequest == nil {
		return false, ErrInvalidArgumentObjectType.Msg("get graph request not set (nil)")
	}

	if ok, err := ObjectIdentifier(i.GetGraphRequest.Anchor).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectIdentifier(i.GetGraphRequest.Object).Validate(); !ok {
		return ok, err
	}

	if ok, err := RelationTypeIdentifier(i.GetGraphRequest.Relation).Validate(); !ok {
		return ok, err
	}

	if ok, err := ObjectIdentifier(i.GetGraphRequest.Subject).Validate(); !ok {
		return ok, err
	}

	return true, nil
}

func (i *getGraph) Exec(ctx context.Context) (*dsr.GetGraphResponse, error) {
	return nil, nil
}
