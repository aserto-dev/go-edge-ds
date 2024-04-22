package datasync

import (
	"context"

	dsc "github.com/aserto-dev/go-directory/pkg/datasync"
	"google.golang.org/grpc"
)

type Client struct {
}

var _ dsc.Client = &Client{}

func (c *Client) Sync(ctx context.Context, conn *grpc.ClientConn, opts ...dsc.Option) error {
	return nil
}
