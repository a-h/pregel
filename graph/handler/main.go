package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/99designs/gqlgen/handler"
	"github.com/a-h/pregel"
	"github.com/a-h/pregel/graph"
	"github.com/akrylysov/algnhsa"
)

func main() {
	region := os.Getenv("PREGEL_DYNAMO_REGION")
	shouldQuit := false
	if region == "" {
		fmt.Println("PREGEL_DYNAMO_REGION not set")
		shouldQuit = true
	}
	tableName := os.Getenv("PREGEL_DYNAMO_TABLE_NAME")
	if tableName == "" {
		fmt.Println("PREGEL_DYNAMO_TABLE_NAME is not set")
		shouldQuit = true
	}
	if shouldQuit {
		os.Exit(1)
	}

	store, err := pregel.NewStore(region, tableName)
	if err != nil {
		log.Fatal(err)
	}
	store.RegisterDataType(func() interface{} {
		return &graph.Location{}
	})

	http.Handle("/", handler.Playground("GraphQL playground", "/query"))
	root := &graph.Resolver{
		MutationResolver: &graph.PregelMutationResolver{
			Store: store,
		},
		NodeResolver:  &graph.PregelNodeResolver{},
		QueryResolver: &graph.PregelQueryResolver{},
	}

	h := handler.GraphQL(graph.NewExecutableSchema(graph.Config{Resolvers: root}))
	statsLogger := func(stats graph.NodeDataLoaderStats) {
		log.Printf("stats: %+v\n", stats)
	}
	http.Handle("/query", graph.WithNodeDataloaderMiddleware(store, statsLogger, h))

	algnhsa.ListenAndServe(http.DefaultServeMux, nil)
}
