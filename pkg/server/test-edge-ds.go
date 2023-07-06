package server

import (
	"context"
	"net"

	dse2 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	dsi2 "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"

	dse3 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v3"
	dsi3 "github.com/aserto-dev/go-directory/aserto/directory/importer/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"

	eds "github.com/aserto-dev/go-edge-ds"
	"github.com/aserto-dev/go-edge-ds/pkg/directory"
	"github.com/rs/zerolog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type TestEdgeClient struct {
	V2 ClientV2
	V3 ClientV3
}

type ClientV2 struct {
	Reader   dsr2.ReaderClient
	Writer   dsw2.WriterClient
	Importer dsi2.ImporterClient
	Exporter dse2.ExporterClient
}

type ClientV3 struct {
	Reader   dsr3.ReaderClient
	Writer   dsw3.WriterClient
	Importer dsi3.ImporterClient
	Exporter dse3.ExporterClient
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
	dsr2.RegisterReaderServer(s, edgeDirServer.Reader2())
	dsw2.RegisterWriterServer(s, edgeDirServer.Writer2())
	dse2.RegisterExporterServer(s, edgeDirServer.Exporter2())
	dsi2.RegisterImporterServer(s, edgeDirServer.Importer2())

	dsr3.RegisterReaderServer(s, edgeDirServer.Reader3())
	dsw3.RegisterWriterServer(s, edgeDirServer.Writer3())
	dse3.RegisterExporterServer(s, edgeDirServer.Exporter3())
	dsi3.RegisterImporterServer(s, edgeDirServer.Importer3())

	go func() {
		if err := s.Serve(listener); err != nil {
			panic(err)
		}
	}()

	conn, _ := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())

	client := TestEdgeClient{
		V2: ClientV2{
			Reader:   dsr2.NewReaderClient(conn),
			Writer:   dsw2.NewWriterClient(conn),
			Importer: dsi2.NewImporterClient(conn),
			Exporter: dse2.NewExporterClient(conn),
		},
		V3: ClientV3{
			Reader:   dsr3.NewReaderClient(conn),
			Writer:   dsw3.NewWriterClient(conn),
			Importer: dsi3.NewImporterClient(conn),
			Exporter: dse3.NewExporterClient(conn),
		},
	}

	return &client, s.Stop
}
