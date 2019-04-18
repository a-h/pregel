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

	store, err := pregel.NewStore("eu-west-2", "pregelStoreLocal")
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/", handler.Playground("GraphQL playground", "/query"))
	root := &graph.Resolver{
		MutationResolver: &graph.PregelMutationResolver{
			Store: store,
		},
		NodeResolver: &graph.PregelNodeResolver{
			Store: store,
		},
		QueryResolver: &graph.PregelQueryResolver{
			Store: store,
		},
	}

	http.Handle("/query", handler.GraphQL(graph.NewExecutableSchema(graph.Config{Resolvers: root})))

	log.Printf("connect to http://localhost:%s/ for GraphQL playground", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
