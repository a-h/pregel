package pregel

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/a-h/pregel/db"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func newMockDynamoDBClient() *mockDynamoDBClient {
	return &mockDynamoDBClient{}
}

type mockDynamoDBClient struct {
	errorToReturn error
	batchDeleter  func(keys []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error)
	batchPutter   func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error)
	queryByIDer   func(idField, idValue string) (items []map[string]*dynamodb.AttributeValue, cc db.ConsumedCapacity, err error)
}

func (mdc *mockDynamoDBClient) BatchDelete(keys []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
	return mdc.batchDeleter(keys)
}

func (mdc *mockDynamoDBClient) BatchPut(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
	return mdc.batchPutter(items)
}

func (mdc *mockDynamoDBClient) QueryByID(idField, idValue string) (items []map[string]*dynamodb.AttributeValue, cc db.ConsumedCapacity, err error) {
	return mdc.queryByIDer(idField, idValue)
}

type testNodeData struct {
	ExtraAttribute string `json:"extra"`
}

type testEdgeData struct {
	EdgeDataField int `json:"edgeDataField"`
}

func TestStorePut(t *testing.T) {
	const tableName = "dynamoTableName"

	tests := []struct {
		name            string
		node            Node
		expectedItems   []map[string]*dynamodb.AttributeValue
		mockOutputError error
		expectedErr     error
	}{
		{
			name:        "Missing node ID results in an error",
			node:        NewNode(""),
			expectedErr: ErrMissingNodeID,
		},
		{
			name: "Put node without data results in a simple node write",
			node: NewNode("id"),
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("id"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
			mockOutputError: nil,
		},
		{
			name: "Put node with data results in two writes, the node itself, plus a data record",
			node: NewNode("id").WithData(testNodeData{
				ExtraAttribute: "ExtraValue",
			}),
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("id"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("id"),
					},
					"rng": {
						S: aws.String("node/data/testNodeData"),
					},
					"t": {
						S: aws.String("testNodeData"),
					},
					"extra": {
						S: aws.String("ExtraValue"),
					},
				},
			},
			mockOutputError: nil,
		},
		{
			name: "Put node with a child edge results in 3 writes, the node itself, plus two edge records. " +
				"One edge for the parent to let it know it has a child, and one for the child to let it know it has a parent",
			node: NewNode("parentNode").WithChildren(NewEdge("childNode")),
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode"),
					},
				},
			},
			mockOutputError: nil,
		},
		{
			name: "Put node with a parent edge results in 3 writes, the node itself, plus two edge records. " +
				"One edge for the child to let it know it has a parent, and one for the parent to let it know it has a child",
			node: NewNode("childNode").WithParents(NewEdge("parentNode")),
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
			},
			mockOutputError: nil,
		},
		{
			name: "Edges can have data records associated with them. " +
				"This results in two extra records, one for the edge in each direction.",
			node: NewNode("parentNode").WithChildren(NewEdge("childNode").WithData(testEdgeData{
				EdgeDataField: 123,
			})),
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode/data/testEdgeData"),
					},
					"t": {
						S: aws.String("testEdgeData"),
					},
					"edgeDataField": {
						N: aws.String(strconv.FormatInt(123, 10)),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode/data/testEdgeData"),
					},
					"t": {
						S: aws.String("testEdgeData"),
					},
					"edgeDataField": {
						N: aws.String(strconv.FormatInt(123, 10)),
					},
				},
			},
			mockOutputError: nil,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newMockDynamoDBClient()
			var actualItems []map[string]*dynamodb.AttributeValue
			callCount := 0
			client.batchPutter = func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected BatchPut to be called once, but was called %d times", callCount+1)
				}
				actualItems = items
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.mockOutputError
			}
			s := NewStoreWithClient(client)
			err := s.Put(test.node)
			if err != test.expectedErr {
				t.Errorf("expected err %v, got %v", test.expectedErr, err)
			}
			if !reflect.DeepEqual(actualItems, test.expectedItems) {
				t.Errorf("\nexpected:\n%s\n\ngot:\n%s\n", format(test.expectedItems), format(actualItems))
			}
		})
	}
}

func TestStorePutNodeData(t *testing.T) {
	const tableName = "dynamoTableName"

	tests := []struct {
		name string
		// id of the node to add data to
		id              string
		data            Data
		expectedItems   []map[string]*dynamodb.AttributeValue
		mockOutputError error
		expectedErr     error
	}{
		{
			name:        "Missing node ID results in an error",
			id:          "",
			expectedErr: ErrMissingNodeID,
		},
		{
			name: "Nil data results in no writes",
			id:   "nodeA",
			data: nil,
		},
		{
			name: "No data results in no writes",
			id:   "nodeA",
			data: NewData(),
		},
		{
			name: "Put node with data results in two writes, the node itself, plus a data record",
			id:   "nodeA",
			data: NewData(testNodeData{
				ExtraAttribute: "ExtraValue",
			}),
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node/data/testNodeData"),
					},
					"t": {
						S: aws.String("testNodeData"),
					},
					"extra": {
						S: aws.String("ExtraValue"),
					},
				},
			},
			mockOutputError: nil,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newMockDynamoDBClient()
			var actualItems []map[string]*dynamodb.AttributeValue
			callCount := 0
			client.batchPutter = func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected BatchPut to be called once, but was called %d times", callCount+1)
				}
				actualItems = items
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.mockOutputError
			}
			s := NewStoreWithClient(client)
			err := s.PutNodeData(test.id, test.data)
			if err != test.expectedErr {
				t.Errorf("expected err %v, got %v", test.expectedErr, err)
			}
			if !reflect.DeepEqual(actualItems, test.expectedItems) {
				t.Errorf("\nexpected:\n%s\n\ngot:\n%s\n", format(test.expectedItems), format(actualItems))
			}
		})
	}
}

func TestStorePutEdge(t *testing.T) {
	const tableName = "dynamoTableName"

	tests := []struct {
		name            string
		parent          string
		edge            *Edge
		expectedItems   []map[string]*dynamodb.AttributeValue
		mockOutputError error
		expectedErr     error
	}{
		{
			name:        "Missing parent ID results in an error",
			parent:      "",
			expectedErr: ErrMissingNodeID,
		},
		{
			name:   "Edge with no data results in two writes, a parent and a child",
			parent: "parentNode",
			edge:   NewEdge("childNode"),
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode"),
					},
				},
			},
		},
		{
			name:   "An edge with data results in 4 writes writes, a parent and a child, plus an edge data record for each",
			parent: "parentNode",
			edge: NewEdge("childNode").WithData(&testEdgeData{
				EdgeDataField: 123,
			}),
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode/data/testEdgeData"),
					},
					"t": {
						S: aws.String("testEdgeData"),
					},
					"edgeDataField": {
						N: aws.String(strconv.Itoa(123)),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode/data/testEdgeData"),
					},
					"t": {
						S: aws.String("testEdgeData"),
					},
					"edgeDataField": {
						N: aws.String(strconv.Itoa(123)),
					},
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newMockDynamoDBClient()
			var actualItems []map[string]*dynamodb.AttributeValue
			callCount := 0
			client.batchPutter = func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected BatchPut to be called once, but was called %d times", callCount+1)
				}
				actualItems = items
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.mockOutputError
			}
			s := NewStoreWithClient(client)
			err := s.PutEdges(test.parent, test.edge)
			if err != test.expectedErr {
				t.Errorf("expected err %v, got %v", test.expectedErr, err)
			}
			if !reflect.DeepEqual(actualItems, test.expectedItems) {
				t.Errorf("\nexpected:\n%s\n\ngot:\n%s\n", format(test.expectedItems), format(actualItems))
			}
		})
	}
}

func TestStorePutEdgeData(t *testing.T) {
	const tableName = "dynamoTableName"

	tests := []struct {
		name            string
		parent          string
		child           string
		data            Data
		expectedItems   []map[string]*dynamodb.AttributeValue
		mockOutputError error
		expectedErr     error
	}{
		{
			name:        "Missing parent ID results in an error",
			parent:      "",
			child:       "childNode",
			expectedErr: ErrMissingNodeID,
		},
		{
			name:        "Missing child ID results in an error",
			parent:      "parentNode",
			child:       "",
			expectedErr: ErrMissingNodeID,
		},
		{
			name:   "No data still adds the node connection",
			parent: "parentNode",
			child:  "childNode",
			data:   nil,
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode"),
					},
				},
			},
		},
		{
			name:   "Valid data results in 4 writes writes containing a copy of the edge data record for each side of the relationship",
			parent: "parentNode",
			child:  "childNode",
			data: NewData(&testEdgeData{
				EdgeDataField: 123,
			}),
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/childNode/data/testEdgeData"),
					},
					"t": {
						S: aws.String("testEdgeData"),
					},
					"edgeDataField": {
						N: aws.String(strconv.Itoa(123)),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/parentNode/data/testEdgeData"),
					},
					"t": {
						S: aws.String("testEdgeData"),
					},
					"edgeDataField": {
						N: aws.String(strconv.Itoa(123)),
					},
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newMockDynamoDBClient()
			var actualItems []map[string]*dynamodb.AttributeValue
			callCount := 0
			client.batchPutter = func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected BatchPut to be called once, but was called %d times", callCount+1)
				}
				actualItems = items
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.mockOutputError
			}
			s := NewStoreWithClient(client)
			err := s.PutEdgeData(test.parent, test.child, test.data)
			if err != test.expectedErr {
				t.Errorf("expected err %v, got %v", test.expectedErr, err)
			}
			if !reflect.DeepEqual(actualItems, test.expectedItems) {
				t.Errorf("\nexpected:\n%s\n\ngot:\n%s\n", format(test.expectedItems), format(actualItems))
			}
		})
	}
}

func format(v []map[string]*dynamodb.AttributeValue) string {
	var b bytes.Buffer
	for _, vv := range v {
		b.WriteString(fmt.Sprintf("%+v\n", vv))
	}
	return b.String()
}
