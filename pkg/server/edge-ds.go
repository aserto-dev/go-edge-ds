package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/aserto-dev/certs"
	dse "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	dsi "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	dsr "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	eds "github.com/aserto-dev/go-edge-ds"
	edgeDirectory "github.com/aserto-dev/go-edge-ds/pkg/directory"
	"github.com/aserto-dev/go-edge-ds/pkg/session"
	"github.com/pkg/errors"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const connectionTimeout time.Duration = 5 * time.Second

type edgeServer struct {
	server   *grpc.Server
	edgeDir  *edgeDirectory.Directory
	host     string
	grpcPort int
	logger   *zerolog.Logger
}

func NewEdgeServer(cfg edgeDirectory.Config, certCfg *certs.TLSCredsConfig, host string, grpcPort int, logger *zerolog.Logger) (*edgeServer, error) {

	edgeDSLogger := logger.With().Str("component", "api.edge-directory").Logger()

	edgeDirServer, err := eds.New(&cfg, &edgeDSLogger)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create edge directory server")
	}

	sessionMiddleware := session.HeaderMiddleware{DisableValidation: false}

	opts := []grpc.ServerOption{grpc.ConnectionTimeout(connectionTimeout),
		grpc.UnaryInterceptor(sessionMiddleware.Unary()),
		grpc.StreamInterceptor(sessionMiddleware.Stream()),
	}
	if certCfg != nil {
		tlsCreds, err := certs.GRPCServerTLSCreds(*certCfg)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get TLS credentials")
		}
		tlsAuth := grpc.Creds(tlsCreds)
		opts = append(opts, tlsAuth)
	}
	s := grpc.NewServer(opts...)

	dsr.RegisterReaderServer(s, edgeDirServer)
	dsw.RegisterWriterServer(s, edgeDirServer)
	dse.RegisterExporterServer(s, edgeDirServer)
	dsi.RegisterImporterServer(s, edgeDirServer)

	reflection.Register(s)
	return &edgeServer{server: s,
			edgeDir:  edgeDirServer,
			host:     host,
			grpcPort: grpcPort,
			logger:   &edgeDSLogger},
		nil
}

func (s *edgeServer) Start(ctx context.Context) error {
	s.logger.Info().Str("host", s.host).Int("port", s.grpcPort).Msg("Starting edge directory server")

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
