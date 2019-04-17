// Code generated by github.com/99designs/gqlgen, DO NOT EDIT.

package graph

type AnyNode interface {
	IsAnyNode()
}

type Connection interface {
	IsConnection()
}

type Edge interface {
	IsEdge()
}

type Node interface {
	IsNode()
}

type NewEdge struct {
	Parent   string   `json:"parent"`
	Children []string `json:"children"`
}

type NewNode struct {
	ID       string   `json:"id"`
	Parents  []string `json:"parents"`
	Children []string `json:"children"`
}

type PageInfo struct {
	EndCursor       *string `json:"endCursor"`
	HasNextPage     bool    `json:"hasNextPage"`
	HasPreviousPage bool    `json:"hasPreviousPage"`
	StartCursor     *string `json:"startCursor"`
}

type SimpleConnection struct {
	Edges      []SimpleEdge `json:"edges"`
	PageInfo   PageInfo     `json:"pageInfo"`
	TotalCount int          `json:"totalCount"`
}

func (SimpleConnection) IsConnection() {}

type SimpleEdge struct {
	Cursor string `json:"cursor"`
	Node   Node   `json:"node"`
}

func (SimpleEdge) IsEdge() {}

type SimpleNode struct {
	ID       string            `json:"id"`
	Parent   Node              `json:"parent"`
	Parents  *SimpleConnection `json:"parents"`
	Children *SimpleConnection `json:"children"`
}

func (SimpleNode) IsNode()    {}
func (SimpleNode) IsAnyNode() {}
