package ds

import (
	"context"

	azm "github.com/aserto-dev/azm"
	mv2 "github.com/aserto-dev/azm/v2"
	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	"github.com/aserto-dev/go-edge-ds/pkg/bdb"

	bolt "go.etcd.io/bbolt"
)

type model struct {
	*azm.Model
}

func Model(m *azm.Model) *model { return &model{m} }

func (m *model) Update(ctx context.Context, tx *bolt.Tx) (*azm.Model, error) {
	pageReq := &dsc.PaginationRequest{Size: 1000}

	objTypes, _, err := bdb.List[dsc.ObjectType](ctx, tx, bdb.ObjectTypesPath, pageReq)
	if err != nil {
		return nil, err
	}

	relTypes, _, err := bdb.List[dsc.RelationType](ctx, tx, bdb.RelationTypesPath, pageReq)
	if err != nil {
		return nil, err
	}

	return mv2.Model().Update(objTypes, relTypes)
}
