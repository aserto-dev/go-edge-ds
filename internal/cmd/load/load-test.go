package main

import (
	"context"

	dsc2 "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsw2 "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aserto-dev/go-aserto/client"

	"encoding/json"
	"log"
	"os"
)

type Loader struct {
	Objects   []*dsc2.Object   `json:"objects"`
	Relations []*dsc2.Relation `json:"relations"`
}

func main() {
	b, err := os.ReadFile("./tests.json")
	if err != nil {
		log.Fatalln(err)
	}
	var loader Loader

	if err := json.Unmarshal(b, &loader); err != nil {
		log.Fatalln(err)
	}

	ctx := context.Background()
	conn, err := client.NewConnection(
		ctx,
		client.WithAddr("localhost:9292"),
		client.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		client.WithSessionID(uuid.NewString()),
	)
	if err != nil {
		log.Fatalln(err)
	}

	writer := dsw2.NewWriterClient(conn.Conn)

	for _, object := range loader.Objects {
		resp, err := writer.SetObject(ctx, &dsw2.SetObjectRequest{Object: object})
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("%s:%s\n", resp.Result.Type, resp.Result.Key)
	}

	for _, relation := range loader.Relations {
		resp, err := writer.SetRelation(ctx, &dsw2.SetRelationRequest{Relation: relation})
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("%s:%s|%s|%s:%s\n",
			resp.Result.Object.GetType(),
			resp.Result.Object.GetKey(),
			resp.Result.Relation,
			resp.Result.Subject.GetType(),
			resp.Result.Subject.GetKey(),
		)
	}
}
