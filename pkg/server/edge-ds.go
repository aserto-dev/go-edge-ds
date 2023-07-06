package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/aserto-dev/certs"

	dse2 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	dsi2 "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	dsr2 "github.com/aserto-dev/go-directory/aserto/directory/reader/v2"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"

	dse3 "github.com/aserto-dev/go-directory/aserto/directory/exporter/v3"
	dsi3 "github.com/aserto-dev/go-directory/aserto/directory/importer/v3"
	dsr3 "github.com/aserto-dev/go-directory/aserto/directory/reader/v3"
	dsw3 "github.com/aserto-dev/go-directory/aserto/directory/writer/v3"

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

	edgeDSLogger := logger.With().Str("component", "api.edge-directory").Logger().Level(zerolog.InfoLevel)

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

	dsr2.RegisterReaderServer(s, edgeDirServer.Reader2())
	dsw2.RegisterWriterServer(s, edgeDirServer.Writer2())
	dse2.RegisterExporterServer(s, edgeDirServer.Exporter2())
	dsi2.RegisterImporterServer(s, edgeDirServer.Importer2())

	dsr3.RegisterReaderServer(s, edgeDirServer.Reader3())
	dsw3.RegisterWriterServer(s, edgeDirServer.Writer3())
	dse3.RegisterExporterServer(s, edgeDirServer.Exporter3())
	dsi3.RegisterImporterServer(s, edgeDirServer.Importer3())

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
