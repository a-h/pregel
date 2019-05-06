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

// SaveNode saves Nodes.
func (pr *PregelMutationResolver) SaveNode(ctx context.Context, input SaveNodeInput) (output *SaveNodeOutput, err error) {
	n := pregel.NewNode(input.ID)
	for _, p := range input.Parents {
		n = n.WithParents(pregel.NewEdge(p))
	}
	for _, c := range input.Children {
		n = n.WithChildren(pregel.NewEdge(c))
	}
	if input.Location != nil {
		n = n.WithData(Location{
			Lat: input.Location.Lat,
			Lng: input.Location.Lng,
		})
	}
	err = pr.Store.Put(n)
	if err != nil {
		return
	}
	output = &SaveNodeOutput{
		ID: input.ID,
	}
	return
}

// SaveEdge saves edges.
func (pr *PregelMutationResolver) SaveEdge(ctx context.Context, input SaveEdgeInput) (output *SaveEdgeOutput, err error) {
	// Copy GraphQL edge data to pregel edge.
	e := pregel.NewEdge(input.Child)
	if input.Location != nil {
		e = e.WithData(input.Location)
	}
	err = pr.Store.PutEdges(input.Parent, e)
	if err != nil {
		return
	}
	output = &SaveEdgeOutput{
		Parent: input.Parent,
		Child:  input.Child,
	}
	return
}

// RemoveNode from the database.
func (pr *PregelMutationResolver) RemoveNode(ctx context.Context, input RemoveNodeInput) (output *RemoveNodeOutput, err error) {
	err = pr.Store.Delete(input.ID)
	output = &RemoveNodeOutput{}
	if err == nil {
		output.Removed = true
	}
	return
}

// RemoveEdge from the database.
func (pr *PregelMutationResolver) RemoveEdge(ctx context.Context, input RemoveEdgeInput) (output *RemoveEdgeOutput, err error) {
	err = pr.Store.DeleteEdge(input.Parent, input.Child)
	output = &RemoveEdgeOutput{}
	if err == nil {
		output.Removed = true
	}
	return
}

// SetNodeFields sets data on a node.
func (pr *PregelMutationResolver) SetNodeFields(ctx context.Context, input SetNodeFieldsInput) (output *SetNodeFieldsOutput, err error) {
	output = &SetNodeFieldsOutput{}
	if input.Location == nil {
		return
	}
	// Convert the input into the standard type.
	location := Location{
		Lat: input.Location.Lat,
		Lng: input.Location.Lng,
	}
	err = pr.Store.PutNodeData(input.ID, pregel.NewData(location))
	if err == nil {
		output.Set = true
	}
	return
}

// SetEdgeFields sets data against edges.
func (pr *PregelMutationResolver) SetEdgeFields(ctx context.Context, input SetEdgeFieldsInput) (output *SetEdgeFieldsOutput, err error) {
	output = &SetEdgeFieldsOutput{}
	if input.Location == nil {
		return
	}
	// Convert the input into the standard type.
	location := Location{
		Lat: input.Location.Lat,
		Lng: input.Location.Lng,
	}
	err = pr.Store.PutEdgeData(input.Parent, input.Child, pregel.NewData(location))
	if err == nil {
		output.Set = true
	}
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

// Data converts the underlying pregel.Node's data into the GraphQL data.
func (r *PregelNodeResolver) Data(ctx context.Context, obj *pregel.Node) (items []NodeDataItem, err error) {
	// Convert the data into NodeDataItem values.
	for _, v := range obj.Data {
		if itm, ok := v.(NodeDataItem); ok {
			items = append(items, itm)
		}
	}
	return
}

func filterEdges(edges []*pregel.Edge, first int, after *string) (filtered []*pregel.Edge, pi PageInfo) {
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

func createConnectionFrom(ctx context.Context, edges []*pregel.Edge, first int, after *string) (c *Connection, err error) {
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
		if n == nil {
			//TODO: Log the fact that we received an unexpected null record for one of the keys.
			continue
		}
		ee := Edge{
			Cursor: gqlid.Encode(n.ID),
			Node:   n,
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
