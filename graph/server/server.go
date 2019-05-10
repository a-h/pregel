package main

import (
	"log"
	"net/http"
	"os"

	"github.com/99designs/gqlgen/handler"
	"github.com/a-h/pregel"
	"github.com/a-h/pregel/graph"
)

const defaultPort = "8080"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	store, err := pregel.NewDynamoStore("eu-west-2", "pregelStoreLocal")
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

	log.Printf("connect to http://localhost:%s/ for GraphQL playground", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
