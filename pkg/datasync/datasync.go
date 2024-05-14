package datasync

import (
	"context"
	"fmt"

	dsc "github.com/aserto-dev/go-directory/pkg/datasync"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

type Client struct {
	logger *zerolog.Logger
}

var _ dsc.Client = &Client{}

func New(logger *zerolog.Logger) *Client {
	return &Client{logger: logger}
}

func (c *Client) Sync(ctx context.Context, conn *grpc.ClientConn, opts ...dsc.Option) error {
	options := &dsc.Options{}
	for _, f := range opts {
		f(options)
	}

	for _, flag := range []dsc.Mode{dsc.Manifest, dsc.Full, dsc.Diff, dsc.Watermark} {
		c.logger.Info().Bool(flag.String(), dsc.Has(options.Mode, flag)).Msg("modes")
	}

	switch {
	case dsc.Has(options.Mode, dsc.Manifest):
		c.logger.Info().Str("mode", "manifest").Msg("datasync.sync")

	case dsc.Has(options.Mode, dsc.Full):
		c.logger.Info().Str("mode", "full").Msg("datasync.sync")

	case dsc.Has(options.Mode, dsc.Diff):
		c.logger.Info().Str("mode", "diff").Msg("datasync.sync")

	case dsc.Has(options.Mode, dsc.Watermark):
		c.logger.Info().Str("mode", "watermark").Msg("datasync.sync")
	default:
		return fmt.Errorf("unknown sync mode")
	}

	return nil
}
