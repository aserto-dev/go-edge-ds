package main

import (
	"context"

	dsc "github.com/aserto-dev/go-directory/aserto/directory/common/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aserto-dev/aserto-go/client"

	"encoding/json"
	"log"
	"os"
)

type Loader struct {
	Objects   []*dsc.Object   `json:"objects"`
	Relations []*dsc.Relation `json:"relations"`
}

func main() {
	b, err := os.ReadFile("./tests.json")
	if err != nil {
		log.Println(err)
	}
	var loader Loader

	if err := json.Unmarshal(b, &loader); err != nil {
		log.Println(err)
	}

	ctx := context.Background()
	conn, err := client.NewConnection(
		ctx,
		client.WithAddr("localhost:9292"),
		client.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		client.WithSessionID(uuid.NewString()),
	)
	if err != nil {
		log.Println(err)
	}

	writer := dsw.NewWriterClient(conn.Conn)

	for _, object := range loader.Objects {
		resp, err := writer.SetObject(ctx, &dsw.SetObjectRequest{Object: object})
		if err != nil {
			log.Println(err)
		}
		log.Printf("%s:%s\n", resp.Result.Type, resp.Result.Id)
	}

	for _, relation := range loader.Relations {
		resp, err := writer.SetRelation(ctx, &dsw.SetRelationRequest{Relation: relation})
		if err != nil {
			log.Println(err)
		}
		log.Printf("%s:%s|%s:%s|%s|%s\n",
			resp.Result.Subject.GetType(),
			resp.Result.Subject.GetId(),
			resp.Result.Object.GetType(),
			resp.Result.Relation,
			resp.Result.Object.GetType(),
			resp.Result.Object.GetId(),
		)
	}
}
