package main

import (
	"context"
	"log"
	"os"
	"path"
	"time"

	ds "github.com/aserto-dev/go-edge-ds/pkg/directory"
	"github.com/aserto-dev/go-edge-ds/pkg/server"
	"github.com/rs/zerolog"
	flag "github.com/spf13/pflag"
)

var (
	dbPath string
	port   int
	seed   bool
)

func main() {
	flag.StringVar(&dbPath, "db_path", "", "database file path")
	flag.IntVar(&port, "port", 9292, "port number")
	flag.BoolVar(&seed, "seed", false, "seed metadata objects")
	flag.Parse()

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get current working directory: %v", err)
	}

	config := ds.Config{
		DBPath:         path.Join(cwd, dbPath),
		RequestTimeout: time.Second * 2,
		Seed:           seed,
	}

	logger := zerolog.New(os.Stdout)

	edge := server.NewEdgeServer(config, nil, "localhost", port, &logger)

	defer func() { _ = edge.Stop(context.Background()) }()

	if err := edge.Start(context.Background()); err != nil {
		log.Printf("failed to serve: %v", err)
	}
}
