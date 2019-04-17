package graph

import (
	"context"
) // THIS CODE IS A STARTING POINT ONLY. IT WILL NOT BE UPDATED WITH SCHEMA CHANGES.

type Resolver struct{}

func (r *Resolver) Mutation() MutationResolver {
	return &mutationResolver{r}
}
func (r *Resolver) Query() QueryResolver {
	return &queryResolver{r}
}

type mutationResolver struct{ *Resolver }

func (r *mutationResolver) CreateNode(ctx context.Context, node NewNode) (string, error) {
	panic("not implemented")
}
func (r *mutationResolver) CreateEdges(ctx context.Context, edge NewEdge) (string, error) {
	panic("not implemented")
}

type queryResolver struct{ *Resolver }

func (r *queryResolver) Get(ctx context.Context, id string) (*SimpleNode, error) {
	panic("not implemented")
}
