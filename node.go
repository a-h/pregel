package pregel

// Node within the graph.
type Node struct {
	ID       string      `json:"id"`
	Data     interface{} `json:"data"`
	Children []Edge      `json:"children"`
	Parents  []Edge      `json:"parents"`
}

// WithData adds data to the node.
func (n Node) WithData(data interface{}) Node {
	n.Data = data
	return n
}

// WithParents adds parents to the node.
func (n Node) WithParents(parents ...Edge) Node {
	n.Parents = parents
	return n
}

// WithChildren adds children to the node.
func (n Node) WithChildren(children ...Edge) Node {
	n.Children = children
	return n
}

// NewNode creates a new Node.
func NewNode(id string) Node {
	return Node{
		ID: id,
	}
}

// Edge relationship.
type Edge struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"`
}

// NewEdge creates an edge.
func NewEdge(id string) Edge {
	return Edge{
		ID: id,
	}
}

// WithData adds data to the edge.
func (e Edge) WithData(data interface{}) Edge {
	e.Data = data
	return e
}
