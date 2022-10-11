package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/aserto-dev/certs"
	eds "github.com/aserto-dev/edge-ds"
	edgeDirectory "github.com/aserto-dev/edge-ds/pkg/directory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/aserto-dev/edge-ds/pkg/session"
	"github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	"github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	"github.com/aserto-dev/go-directory/aserto/directory/v2"
	"github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
)

const connectionTimeout time.Duration = 5 * time.Second

type edgeServer struct {
	server   *grpc.Server
	edgeDir  *edgeDirectory.Directory
	host     string
	grpcPort int
	logger   *zerolog.Logger
}

func NewEdgeServer(cfg edgeDirectory.Config, certCfg *certs.TLSCredsConfig, host string, grpcPort int, logger *zerolog.Logger) *edgeServer {

	edgeDSLogger := logger.With().Str("component", "api.edge-directory").Logger()

	edgeDirServer, err := eds.New(&cfg, &edgeDSLogger)
	if err != nil {
		logger.Error().Err(err).Msg("failed to start edge directory server")
	}

	sessionMiddleware := session.HeaderMiddleware{DisableValidation: false}

	opts := []grpc.ServerOption{grpc.ConnectionTimeout(connectionTimeout),
		grpc.UnaryInterceptor(sessionMiddleware.Unary()),
		grpc.StreamInterceptor(sessionMiddleware.Stream()),
	}
	if certCfg != nil {
		tlsCreds, err := certs.GRPCServerTLSCreds(*certCfg)
		if err != nil {
			logger.Error().Err(err).Msg("failed to get tls")
		}
		tlsAuth := grpc.Creds(tlsCreds)
		opts = append(opts, tlsAuth)
	}
	s := grpc.NewServer(opts...)

	directory.RegisterDirectoryServer(s, edgeDirServer)
	writer.RegisterWriterServer(s, edgeDirServer)
	exporter.RegisterExporterServer(s, edgeDirServer)
	importer.RegisterImporterServer(s, edgeDirServer)

	reflection.Register(s)
	return &edgeServer{server: s,
		edgeDir:  edgeDirServer,
		host:     host,
		grpcPort: grpcPort,
		logger:   &edgeDSLogger}
}

func (s *edgeServer) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting edge directory server")

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.host, s.grpcPort))
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to listen on port")
		return err
	}

	if err := s.server.Serve(lis); err != nil {
		s.logger.Error().Err(err).Msg("failed to serve on port")
	}

	return nil
}

func (s *edgeServer) Stop(ctx context.Context) error {
	s.logger.Info().Msg("Stopping edge directory server")
	s.edgeDir.Close()
	s.server.Stop()
	return nil
}
