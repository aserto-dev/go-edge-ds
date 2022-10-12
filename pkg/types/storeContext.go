package types

import (
	"context"

	"github.com/aserto-dev/edge-ds/pkg/boltdb"
)

type StoreContext struct {
	Context context.Context
	Store   *boltdb.BoltDB
	Opts    []boltdb.Opts
}
