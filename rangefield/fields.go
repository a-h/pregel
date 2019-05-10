package rangefield

import (
	"bytes"
	"net/url"
	"strings"
)

// Decode a range field.
func Decode(s string) (f RangeField, ok bool) {
	parts, ok := decodeField(s)
	if !ok {
		return
	}
	switch parts[0] {
	case "node":
		return decodeNodeField(parts[1:])
	case "child":
		return decodeChildField(parts[1:])
	case "parent":
		return decodeParentField(parts[1:])
	}
	return nil, false
}

func decodeNodeField(parts []string) (f RangeField, ok bool) {
	if len(parts) == 0 {
		return Node{}, true
	}
	if len(parts) == 2 && parts[0] == "data" {
		return NodeData{
			DataType: parts[1],
		}, true
	}
	return
}

func decodeChildField(parts []string) (f RangeField, ok bool) {
	if len(parts) == 1 {
		return Child{
			Child: parts[0],
		}, true
	}
	if len(parts) == 3 && parts[1] == "data" {
		return ChildData{
			Child:    parts[0],
			DataType: parts[2],
		}, true
	}
	return
}

func decodeParentField(parts []string) (f RangeField, ok bool) {
	if len(parts) == 1 {
		return Parent{
			Parent: parts[0],
		}, true
	}
	if len(parts) == 3 && parts[1] == "data" {
		return ParentData{
			Parent:   parts[0],
			DataType: parts[2],
		}, true
	}
	return
}

// RangeField for a DynamoDB table.
type RangeField interface {
	Encode() string
}

// Node is the range field for a Node record.
type Node struct{}

// Encode to the field to string.
func (k Node) Encode() string {
	return encodeField("node")
}

// NodeData is the range field for a Node's data record.
type NodeData struct {
	DataType string
}

// Encode to the field to string.
func (k NodeData) Encode() string {
	return encodeField("node", "data", k.DataType)
}

// Child is the range field for a Node's child record.
type Child struct {
	Child string
}

// Encode to the field to string.
func (k Child) Encode() string {
	return encodeField("child", k.Child)
}

// ChildData is the range field for a Node's child's data record.
type ChildData struct {
	Child    string
	DataType string
}

// Encode to the field to string.
func (k ChildData) Encode() string {
	return encodeField("child", k.Child, "data", k.DataType)
}

// Parent is the range field for a Node's Parent record.
type Parent struct {
	Parent string
}

// Encode to the field to string.
func (k Parent) Encode() string {
	return encodeField("parent", k.Parent)
}

// ParentData is the range field for a Node's Parent's data record.
type ParentData struct {
	Parent   string
	DataType string
}

// Encode to the field to string.
func (k ParentData) Encode() string {
	return encodeField("parent", k.Parent, "data", k.DataType)
}

func decodeField(v string) (segs []string, ok bool) {
	segs = strings.Split(v, "/")
	var err error
	for i, s := range segs {
		if s == "" {
			return
		}
		segs[i], err = url.PathUnescape(s)
		if err != nil {
			return
		}
	}
	ok = true
	return
}

func encodeField(values ...string) string {
	var buf bytes.Buffer
	for i, v := range values {
		if i > 0 {
			buf.WriteRune('/')
		}
		buf.WriteString(url.PathEscape(v))
	}
	return buf.String()
}
