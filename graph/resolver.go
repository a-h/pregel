package graph

import (
	"context"
	"errors"
	"strings"

	"github.com/a-h/pregel/graph/gqlid"

	"github.com/a-h/pregel"
)

// Resolver of GraphQL queries.
type Resolver struct {
	MutationResolver MutationResolver
	NodeResolver     NodeResolver
	QueryResolver    QueryResolver
}

// Mutation provides the available mutations.
func (r *Resolver) Mutation() MutationResolver {
	return r.MutationResolver
}

// Node provides the Node resolver, used to resolve the subfields of a Node.
func (r *Resolver) Node() NodeResolver {
	return r.NodeResolver
}

// Query provides the available queries.
func (r *Resolver) Query() QueryResolver {
	return r.QueryResolver
}

// PregelMutationResolver resolves mutations.
type PregelMutationResolver struct {
	Store *pregel.Store
}

// CreateNode creates Nodes.
func (pr *PregelMutationResolver) CreateNode(ctx context.Context, node NewNode) (id string, err error) {
	n := pregel.NewNode(node.ID)
	err = pr.Store.Put(n)
	if err != nil {
		return
	}
	id = node.ID
	return
}

// CreateEdge creates edges.
func (pr *PregelMutationResolver) CreateEdge(ctx context.Context, edge NewEdge) (id string, err error) {
	//TODO: Copy edge data to pregel edge.
	err = pr.Store.PutEdges(edge.Parent, pregel.NewEdge(edge.Child))
	if err != nil {
		return
	}
	id = edge.Parent
	return
}

// PregelNodeResolver uses pregel to get the node's parents and children.
type PregelNodeResolver struct{}

// Parents of the Node.
func (r *PregelNodeResolver) Parents(ctx context.Context, obj *pregel.Node, first int, after *string) (c *Connection, err error) {
	return createConnectionFrom(ctx, obj.Parents, first, after)
}

// Children of the Node.
func (r *PregelNodeResolver) Children(ctx context.Context, obj *pregel.Node, first int, after *string) (*Connection, error) {
	return createConnectionFrom(ctx, obj.Children, first, after)
}

func filterEdges(edges []pregel.Edge, first int, after *string) (filtered []pregel.Edge, pi PageInfo) {
	start, end := 0, len(edges)
	if after != nil {
		afterID, err := gqlid.Decode(*after)
		if err == nil {
			for i, e := range edges {
				if e.ID == afterID {
					start = i + 1
					pi.HasPreviousPage = true
					break
				}
			}
		}
	}
	if first > 0 {
		end = start + first
		if end > len(edges) {
			end = len(edges)
		}
		if end < len(edges) {
			pi.HasNextPage = true
		}
	}
	if start != end {
		filtered = edges[start:end]
	}
	if len(filtered) > 0 {
		sc := gqlid.Encode(filtered[0].ID)
		pi.StartCursor = &sc
		ec := gqlid.Encode(filtered[len(filtered)-1].ID)
		pi.EndCursor = &ec
	}
	return
}

func createConnectionFrom(ctx context.Context, edges []pregel.Edge, first int, after *string) (c *Connection, err error) {
	if len(edges) == 0 {
		return
	}
	c = &Connection{
		Edges: []Edge{},
	}
	edges, c.PageInfo = filterEdges(edges, first, after)
	c.TotalCount = len(edges)

	keys := make([]string, len(edges))
	for i, e := range edges {
		keys[i] = e.ID
	}

	nodes, errs := FromContext(ctx).LoadAll(keys)
	err = joinErrs(errs)
	if err != nil {
		return
	}
	for _, n := range nodes {
		ee := Edge{
			Cursor: gqlid.Encode(n.ID),
			// Data: child.Data, // TODO: Add data to the edge.
			Node: n,
		}
		c.Edges = append(c.Edges, ee)
	}
	return
}

func joinErrs(errs []error) error {
	var messages []string
	for _, e := range errs {
		if e != nil {
			messages = append(messages, e.Error())
		}
	}
	if len(messages) > 0 {
		return errors.New(strings.Join(messages, ", "))
	}
	return nil
}

// PregelQueryResolver resolves queries using pregel.
type PregelQueryResolver struct{}

// Get a node by its ID.
func (pr *PregelQueryResolver) Get(ctx context.Context, id string) (n *pregel.Node, err error) {
	return FromContext(ctx).Load(id)
}
