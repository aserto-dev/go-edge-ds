package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path"
	"time"

	ds "github.com/aserto-dev/edge-ds/pkg/directory"
	"github.com/aserto-dev/edge-ds/pkg/server"
	"github.com/rs/zerolog"
)

const connectionTimeout time.Duration = 5 * time.Second

var port int

func main() {
	flag.IntVar(&port, "port", 12345, "port number")
	flag.Parse()

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get current working directory: %v", err)
	}

	config := ds.Config{
		DBPath: path.Join(cwd, ".db", "eds.db"),
		Seed:   true,
	}

	logger := zerolog.New(os.Stdout)

	edge := server.NewEdgeServer(config, nil, "localhost", port, &logger)

	defer edge.Stop(context.Background())

	if err := edge.Start(context.Background()); err != nil {
		log.Printf("failed to serve: %v", err)
	}
}
