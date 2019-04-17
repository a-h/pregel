package pregel

// Node within the graph.
type Node struct {
	ID    string      `json:"id"`
	Data  interface{} `json:"data"`
	Edges []Edge      `json:"edges"`
}

// NewNode creates a new Node.
func NewNode(id string, data interface{}, edges ...Edge) Node {
	return Node{
		ID:    id,
		Data:  data,
		Edges: edges,
	}
}

// Edge containing a child.
type Edge struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"`
}

// NewEdge creates an edge.
func NewEdge(to string, data interface{}) Edge {
	return Edge{
		ID:   to,
		Data: data,
	}
}
