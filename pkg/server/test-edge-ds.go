package server

import (
	"context"
	"net"

	eds "github.com/aserto-dev/edge-ds"
	"github.com/aserto-dev/edge-ds/pkg/directory"
	dse "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	dsi "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/rs/zerolog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type TestEdgeClient struct {
	Reader   dsr.ReaderClient
	Writer   dsw.WriterClient
	Importer dsi.ImporterClient
	Exporter dse.ExporterClient
}

func NewTestEdgeServer(ctx context.Context, logger *zerolog.Logger, cfg *directory.Config) (*TestEdgeClient, func()) {
	buffer := 1024 * 1024
	listener := bufconn.Listen(buffer)

	edgeDSLogger := logger.With().Str("component", "api.edge-directory").Logger()

	edgeDirServer, err := eds.New(cfg, &edgeDSLogger)
	if err != nil {
		logger.Error().Err(err).Msg("failed to start edge directory server")
	}

	s := grpc.NewServer()
	dsr.RegisterReaderServer(s, edgeDirServer)
	dsw.RegisterWriterServer(s, edgeDirServer)
	dse.RegisterExporterServer(s, edgeDirServer)
	dsi.RegisterImporterServer(s, edgeDirServer)

	go func() {
		if err := s.Serve(listener); err != nil {
			panic(err)
		}
	}()

	conn, _ := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())

	client := TestEdgeClient{
		Reader:   dsr.NewReaderClient(conn),
		Writer:   dsw.NewWriterClient(conn),
		Importer: dsi.NewImporterClient(conn),
		Exporter: dse.NewExporterClient(conn),
	}

	return &client, s.Stop
}
