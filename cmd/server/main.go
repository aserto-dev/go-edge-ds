package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"time"

	eds "github.com/aserto-dev/edge-ds"
	ds "github.com/aserto-dev/edge-ds/pkg/directory"
	"github.com/aserto-dev/edge-ds/pkg/session"
	"github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	"github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	"github.com/aserto-dev/go-directory/aserto/directory/v2"
	"github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const connectionTimeout time.Duration = 5 * time.Second

var port int

func main() {
	flag.IntVar(&port, "port", 12345, "port number")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get current working directory: %v", err)
	}

	config := ds.Config{
		DBPath: path.Join(cwd, ".db", "eds.db"),
		Seed:   true,
	}

	logger := zerolog.New(os.Stdout)

	server, err := eds.New(&config, &logger)
	if err != nil {
		log.Fatalf("failed to create service instance: %v", err)
	}
	defer server.Close()

	sessionMiddleware := session.HeaderMiddleware{DisableValidation: false}

	s := grpc.NewServer(
		grpc.ConnectionTimeout(connectionTimeout),
		grpc.UnaryInterceptor(sessionMiddleware.Unary()),
		grpc.StreamInterceptor(sessionMiddleware.Stream()),
	)

	directory.RegisterDirectoryServer(s, server)
	writer.RegisterWriterServer(s, server)
	exporter.RegisterExporterServer(s, server)
	importer.RegisterImporterServer(s, server)

	reflection.Register(s)

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
	}
}
