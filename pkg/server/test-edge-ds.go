package server

import (
	"context"
	"net"

	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"

	"github.com/aserto-dev/aserto-grpc/middlewares/gerr"
	eds "github.com/aserto-dev/go-edge-ds"
	"github.com/aserto-dev/go-edge-ds/pkg/directory"
	"github.com/rs/zerolog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type TestEdgeClient struct {
	V3 ClientV3
}

type ClientV3 struct {
	Reader dsr.ReaderClient
	Writer dsw.WriterClient
}

const bufferSize int = 1024 * 1024

func NewTestEdgeServer(ctx context.Context, logger *zerolog.Logger, cfg *directory.Config) (*TestEdgeClient, func()) {
	listener := bufconn.Listen(bufferSize)

	edgeDSLogger := logger.With().Str("component", "api.edge-directory").Logger()

	edgeDirServer, err := eds.New(context.Background(), cfg, &edgeDSLogger)
	if err != nil {
		logger.Error().Err(err).Msg("failed to start edge directory server")
	}

	errMiddleware := gerr.NewErrorMiddleware()
	s := grpc.NewServer(
		grpc.UnaryInterceptor(errMiddleware.Unary()),
		grpc.StreamInterceptor(errMiddleware.Stream()),
	)

	dsr.RegisterReaderServer(s, edgeDirServer.Reader3())
	dsw.RegisterWriterServer(s, edgeDirServer.Writer3())

	go func() {
		if err := s.Serve(listener); err != nil {
			panic(err)
		}
	}()

	//nolint:staticcheck // bufConn does not seem to work with the default DNS provided by grpc.NewClient.
	conn, _ := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())

	client := TestEdgeClient{
		V3: ClientV3{
			Reader: dsr.NewReaderClient(conn),
			Writer: dsw.NewWriterClient(conn),
		},
	}

	return &client, s.Stop
}
