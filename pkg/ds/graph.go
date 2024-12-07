package ds

import (
	"context"
	"sync"

	"github.com/aserto-dev/azm/cache"
	"github.com/aserto-dev/azm/safe"
	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"

	bolt "go.etcd.io/bbolt"
)

type getGraph struct {
	*safe.SafeGetGraph
}

func GetGraph(i *dsr3.GetGraphRequest) *getGraph {
	return &getGraph{safe.GetGraph(i)}
}

func (i *getGraph) Exec(ctx context.Context, tx *bolt.Tx, mc *cache.Cache) (*dsr3.GetGraphResponse, error) {
	// pool of *dsc3.Relation instances
	msgPool := sync.Pool{
		New: func() interface{} {
			return &dsc3.Relation{}
		},
	}

	// pool of []*dsc3.Relation
	relationsPool := sync.Pool{
		New: func() interface{} {
			return []*dsc3.Relation{}
		},
	}

	resp, err := mc.GetGraph(i.GetGraphRequest, getRelations(ctx, tx, &relationsPool, &msgPool))

	return resp, err
}
