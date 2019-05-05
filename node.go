package pregel

import "reflect"

// Node within the graph.
type Node struct {
	ID   string `json:"id"`
	Data Data   `json:"data"`
	// Children of the node.
	Children []*Edge `json:"children"`
	// Parents of the node.
	Parents []*Edge `json:"parents"`
}

// Data attached to a node or edge.
type Data map[string]interface{}

// WithData adds data to the node.
func (n Node) WithData(v interface{}) Node {
	k := reflect.TypeOf(v).Name()
	return n.WithNamedData(k, v)
}

// WithNamedData adds data to the node, specifying the name to use for storage.
func (n Node) WithNamedData(key string, value interface{}) Node {
	n.Data[key] = value
	return n
}

// WithParents adds parents to the node.
func (n Node) WithParents(parents ...*Edge) Node {
	n.Parents = parents
	return n
}

// WithChildren adds children to the node.
func (n Node) WithChildren(children ...*Edge) Node {
	n.Children = children
	return n
}

// NewNode creates a new Node.
func NewNode(id string) Node {
	return Node{
		ID:   id,
		Data: make(Data),
	}
}

// GetChild edge.
func (n Node) GetChild(id string) *Edge {
	for _, ee := range n.Children {
		if ee.ID == id {
			return ee
		}
	}
	return nil
}

// GetParent edge.
func (n Node) GetParent(id string) *Edge {
	for _, ee := range n.Parents {
		if ee.ID == id {
			return ee
		}
	}
	return nil
}

// Edge relationship.
type Edge struct {
	ID   string `json:"id"`
	Data Data   `json:"data"`
}

// NewEdge creates an edge.
func NewEdge(id string) *Edge {
	return &Edge{
		ID:   id,
		Data: make(Data),
	}
}

// WithData adds data to the edge.
func (e Edge) WithData(v interface{}) *Edge {
	k := reflect.TypeOf(v).Name()
	return e.WithNamedData(k, v)
}

// WithNamedData adds data to the edge.
func (e *Edge) WithNamedData(key string, value interface{}) *Edge {
	e.Data[key] = value
	return e
}
