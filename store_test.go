package pregel

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/a-h/pregel/db"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func newdynamoDBClient() *dynamoDBClient {
	return &dynamoDBClient{}
}

type dynamoDBClient struct {
	errorToReturn error
	batchDeleter  func(keys []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error)
	batchPutter   func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error)
	queryByIDer   func(idField, idValue string) (items []map[string]*dynamodb.AttributeValue, cc db.ConsumedCapacity, err error)
}

func (mdc *dynamoDBClient) BatchDelete(keys []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
	return mdc.batchDeleter(keys)
}

func (mdc *dynamoDBClient) BatchPut(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
	return mdc.batchPutter(items)
}

func (mdc *dynamoDBClient) QueryByID(idField, idValue string) (items []map[string]*dynamodb.AttributeValue, cc db.ConsumedCapacity, err error) {
	return mdc.queryByIDer(idField, idValue)
}

type testNodeData struct {
	ExtraAttribute string `json:"extra"`
}

type testEdgeData struct {
	EdgeDataField int `json:"edgeDataField"`
}

var errTestDatabaseFailure = errors.New("test database failure")

func TestStorePut(t *testing.T) {
	tests := []struct {
		name                 string
		node                 Node
		expectedItems        []map[string]*dynamodb.AttributeValue
		batchPutterOutputErr error
		expectedErr          error
	}{
		{
			name:        "Missing node ID results in an error",
			node:        NewNode(""),
			expectedErr: ErrMissingNodeID,
		},
		{
			name: "database errors are returned",
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
			batchPutterOutputErr: errTestDatabaseFailure,
			expectedErr:          errTestDatabaseFailure,
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
			batchPutterOutputErr: nil,
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
			batchPutterOutputErr: nil,
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
			batchPutterOutputErr: nil,
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
			batchPutterOutputErr: nil,
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
			batchPutterOutputErr: nil,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newdynamoDBClient()
			var actualItems []map[string]*dynamodb.AttributeValue
			callCount := 0
			client.batchPutter = func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected BatchPut to be called once, but was called %d times", callCount+1)
				}
				actualItems = items
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.batchPutterOutputErr
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
	tests := []struct {
		name string
		// id of the node to add data to
		id                   string
		data                 Data
		expectedItems        []map[string]*dynamodb.AttributeValue
		batchPutterOutputErr error
		expectedErr          error
	}{
		{
			name:        "Missing node ID results in an error",
			id:          "",
			expectedErr: ErrMissingNodeID,
		},
		{
			name: "No data results in just the node record",
			id:   "nodeA",
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
		},
		{
			name: "Database errors are returned",
			id:   "nodeA",
			expectedItems: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
			batchPutterOutputErr: errTestDatabaseFailure,
			expectedErr:          errTestDatabaseFailure,
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
						S: aws.String("node"),
					},
				},
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
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newdynamoDBClient()
			var actualItems []map[string]*dynamodb.AttributeValue
			callCount := 0
			client.batchPutter = func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected BatchPut to be called once, but was called %d times", callCount+1)
				}
				actualItems = items
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.batchPutterOutputErr
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
	tests := []struct {
		name                 string
		parent               string
		edge                 *Edge
		expectedItems        []map[string]*dynamodb.AttributeValue
		batchPutterOutputErr error
		expectedErr          error
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
			name:   "Database errors are returned",
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
			batchPutterOutputErr: errTestDatabaseFailure,
			expectedErr:          errTestDatabaseFailure,
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

			client := newdynamoDBClient()
			var actualItems []map[string]*dynamodb.AttributeValue
			callCount := 0
			client.batchPutter = func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected BatchPut to be called once, but was called %d times", callCount+1)
				}
				actualItems = items
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.batchPutterOutputErr
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
	tests := []struct {
		name                 string
		parent               string
		child                string
		data                 Data
		expectedItems        []map[string]*dynamodb.AttributeValue
		batchPutterOutputErr error
		expectedErr          error
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
			name:   "Database errors are returned",
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
			batchPutterOutputErr: errTestDatabaseFailure,
			expectedErr:          errTestDatabaseFailure,
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

			client := newdynamoDBClient()
			var actualItems []map[string]*dynamodb.AttributeValue
			callCount := 0
			client.batchPutter = func(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected BatchPut to be called once, but was called %d times", callCount+1)
				}
				actualItems = items
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.batchPutterOutputErr
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

func TestStoreGet(t *testing.T) {
	tests := []struct {
		name               string
		id                 string
		recordsToReturn    []map[string]*dynamodb.AttributeValue
		expected           Node
		expectedOK         bool
		queryByIDOutputErr error
		expectedErr        error
	}{
		{
			name:       "Missing node ID results in no error, and no results",
			id:         "",
			expectedOK: false,
		},
		{
			name: "A node record can be returned",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
			expected:   NewNode("nodeA"),
			expectedOK: true,
		},
		{
			name: "Database errors are returned",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
			expected:           Node{},
			expectedOK:         false,
			queryByIDOutputErr: errTestDatabaseFailure,
			expectedErr:        errTestDatabaseFailure,
		},
		{
			name: "A node record can be returned with its data",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
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
						N: aws.String("ABC"),
					},
				},
			},
			expected: NewNode("nodeA").WithData(&testNodeData{
				ExtraAttribute: "ABC",
			}),
			expectedOK: true,
		},
		{
			name: "A node record can be returned with its child edges",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeA"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB"),
					},
				},
			},
			expected:   NewNode("nodeA").WithChildren(NewEdge("childNodeA"), NewEdge("childNodeB")),
			expectedOK: true,
		},
		{
			name: "A node record can be returned with its parent edges",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("parent/parentNodeA"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("parent/parentNodeB"),
					},
				},
			},
			expected:   NewNode("nodeA").WithParents(NewEdge("parentNodeA"), NewEdge("parentNodeB")),
			expectedOK: true,
		},
		{
			name: "A node record can be returned with its parents, children, edge data and node data",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
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
						N: aws.String("ABC"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("parent/parentNodeA"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("parent/parentNodeB"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeA"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeA/data/testEdgeData"),
					},
					"t": {
						S: aws.String("testEdgeData"),
					},
					"edgeDataField": {
						N: aws.String(strconv.Itoa(666)),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB"),
					},
				},
			},
			expected: NewNode("nodeA").
				WithData(&testNodeData{ExtraAttribute: "ABC"}).
				WithParents(NewEdge("parentNodeA"), NewEdge("parentNodeB")).
				WithChildren(NewEdge("childNodeA").WithData(&testEdgeData{EdgeDataField: 666}), NewEdge("childNodeB")),
			expectedOK: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newdynamoDBClient()
			callCount := 0
			client.queryByIDer = func(idField, idValue string) ([]map[string]*dynamodb.AttributeValue, db.ConsumedCapacity, error) {
				if idField != "id" {
					t.Errorf("unexpected idField value of '%s'", idField)
				}
				if idValue != test.id {
					t.Errorf("expected id of '%s', got '%s'", test.id, idValue)
				}
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected QueryByID to be called once, but was called %d times", callCount+1)
				}
				cc := db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}
				err := test.queryByIDOutputErr
				return test.recordsToReturn, cc, err
			}
			s := NewStoreWithClient(client)
			s.RegisterDataType(func() interface{} {
				return &testNodeData{}
			})
			s.RegisterDataType(func() interface{} {
				return &testEdgeData{}
			})
			n, ok, err := s.Get(test.id)
			if err != test.expectedErr {
				t.Errorf("expected err %v, got %v", test.expectedErr, err)
			}
			if ok != test.expectedOK {
				t.Errorf("expected OK %v, got %v", test.expectedOK, ok)
			}
			if !reflect.DeepEqual(n, test.expected) {
				t.Errorf("\nexpected:\n%+v\n\ngot:\n%+v\n", test.expected, n)
			}
		})
	}
}

func TestStoreDelete(t *testing.T) {
	tests := []struct {
		name               string
		id                 string
		recordsToReturn    []map[string]*dynamodb.AttributeValue
		keysToDelete       []map[string]*dynamodb.AttributeValue
		queryByIDOutputErr error
		deleteOutputErr    error
		expectedErr        error
	}{
		{
			name:            "Missing node ID results in no error, and no results",
			id:              "",
			deleteOutputErr: fmt.Errorf("unexpected 2nd database call"),
		},
		{
			name: "A node record which doesn't have data or children can be deleted",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
			keysToDelete: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
		},
		{
			name:               "query database errors are returned",
			id:                 "nodeA",
			queryByIDOutputErr: errTestDatabaseFailure,
			expectedErr:        errTestDatabaseFailure,
		},
		{
			name: "delete database errors are returned",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
			keysToDelete: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
			deleteOutputErr: errTestDatabaseFailure,
			expectedErr:     errTestDatabaseFailure,
		},
		{
			name: "A node record with data is fully deleted",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
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
			keysToDelete: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node/data/testNodeData"),
					},
				},
			},
		},
		{
			name: "A node record with children is fully deleted, including deleting the link back from the child to the parent",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
			},
			keysToDelete: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
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
						S: aws.String("parent/nodeA"),
					},
				},
			},
		},
		{
			name: "A node record with children is fully deleted, including deleting the edge data",
			id:   "nodeA",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
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
			},
			keysToDelete: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				// The relationship from the parent to the child.
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNode"),
					},
				},
				// The relationship from the child to the parent.
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/nodeA"),
					},
				},
				// The edge data stored on the parent.
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNode/data/testEdgeData"),
					},
				},
				// The edge data stored on the child.
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNode"),
					},
					"rng": {
						S: aws.String("parent/nodeA/data/testEdgeData"),
					},
				},
			},
		},
		{
			name: "A node record with parents is fully deleted, including deleting the edge data",
			id:   "child",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("child"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("child"),
					},
					"rng": {
						S: aws.String("parent/parentNode"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("child"),
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
			keysToDelete: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("child"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				// The relationship from the child to the parent.
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("child"),
					},
					"rng": {
						S: aws.String("parent/parentNode"),
					},
				},
				// The relationship from the parent to the child.
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/child"),
					},
				},
				// The edge data stored on the child.
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("child"),
					},
					"rng": {
						S: aws.String("parent/parentNode/data/testEdgeData"),
					},
				},
				// The edge data stored on the parent.
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("parentNode"),
					},
					"rng": {
						S: aws.String("child/child/data/testEdgeData"),
					},
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newdynamoDBClient()
			callCount := 0
			client.queryByIDer = func(idField, idValue string) ([]map[string]*dynamodb.AttributeValue, db.ConsumedCapacity, error) {
				if idField != "id" {
					t.Errorf("unexpected idField value of '%s'", idField)
				}
				if idValue != test.id {
					t.Errorf("expected id of '%s', got '%s'", test.id, idValue)
				}
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected QueryByID to be called once, but was called %d times", callCount+1)
				}
				cc := db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}
				err := test.queryByIDOutputErr
				return test.recordsToReturn, cc, err
			}
			client.batchDeleter = func(keys []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				if !reflect.DeepEqual(keys, test.keysToDelete) {
					t.Errorf("\nexpected:\n%+v\n\ngot:\n%+v\n", format(test.keysToDelete), format(keys))
				}
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.deleteOutputErr
			}
			s := NewStoreWithClient(client)
			s.RegisterDataType(func() interface{} {
				return &testNodeData{}
			})
			s.RegisterDataType(func() interface{} {
				return &testEdgeData{}
			})
			err := s.Delete(test.id)
			if err != test.expectedErr {
				t.Errorf("expected err %v, got %v", test.expectedErr, err)
			}
		})
	}
}

func TestStoreDeleteEdge(t *testing.T) {
	tests := []struct {
		name               string
		parent             string
		child              string
		recordsToReturn    []map[string]*dynamodb.AttributeValue
		keysToDelete       []map[string]*dynamodb.AttributeValue
		queryByIDOutputErr error
		deleteOutputErr    error
		expectedErr        error
	}{
		{
			name:               "Missing parent ID results in no error, and no results",
			parent:             "",
			child:              "something",
			queryByIDOutputErr: fmt.Errorf("unexpected 1st database call"),
			deleteOutputErr:    fmt.Errorf("unexpected 2nd database call"),
			expectedErr:        ErrMissingNodeID,
		},
		{
			name:               "Missing child ID results in no error, and no results",
			parent:             "something",
			child:              "",
			queryByIDOutputErr: fmt.Errorf("unexpected 1st database call"),
			deleteOutputErr:    fmt.Errorf("unexpected 2nd database call"),
			expectedErr:        ErrMissingNodeID,
		},
		{
			name:   "A node record which doesn't have any children doesn't delete anything",
			parent: "nodeA",
			child:  "any",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
			},
			deleteOutputErr: fmt.Errorf("unexpected call, shouldn't need to delete anything if there's nothing to delete"),
		},
		{
			name:            "A node which doesn't exist doesn't delete anything",
			parent:          "nodeA",
			child:           "any",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{},
			deleteOutputErr: fmt.Errorf("unexpected call, shouldn't need to delete anything if there's nothing to delete"),
		},
		{
			name:   "A node with children deletes just the matching edges, including deleting the link back from the child to the parent",
			parent: "nodeA",
			child:  "childNodeB",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeA"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB"),
					},
				},
			},
			keysToDelete: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNodeB"),
					},
					"rng": {
						S: aws.String("parent/nodeA"),
					},
				},
			},
		},
		{
			name:               "Query database errors are returned",
			parent:             "nodeA",
			child:              "childNodeB",
			queryByIDOutputErr: errTestDatabaseFailure,
			expectedErr:        errTestDatabaseFailure,
		},
		{
			name:   "Delete database errors are returned",
			parent: "nodeA",
			child:  "childNodeB",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB"),
					},
				},
			},
			keysToDelete: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNodeB"),
					},
					"rng": {
						S: aws.String("parent/nodeA"),
					},
				},
			},
			deleteOutputErr: errTestDatabaseFailure,
			expectedErr:     errTestDatabaseFailure,
		},
		{
			name:   "A node with children with data deletes just the matching edges, including deleting the link back from the child to the parent and data records",
			parent: "nodeA",
			child:  "childNodeB",
			recordsToReturn: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("node"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeA"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB/data/testEdgeData"),
					},
					"t": {
						S: aws.String("testEdgeData"),
					},
					"edgeDataField": {
						N: aws.String(strconv.Itoa(123)),
					},
				},
			},
			keysToDelete: []map[string]*dynamodb.AttributeValue{
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNodeB"),
					},
					"rng": {
						S: aws.String("parent/nodeA"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("nodeA"),
					},
					"rng": {
						S: aws.String("child/childNodeB/data/testEdgeData"),
					},
				},
				map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String("childNodeB"),
					},
					"rng": {
						S: aws.String("parent/nodeA/data/testEdgeData"),
					},
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client := newdynamoDBClient()
			callCount := 0
			client.queryByIDer = func(idField, idValue string) ([]map[string]*dynamodb.AttributeValue, db.ConsumedCapacity, error) {
				if idField != "id" {
					t.Errorf("unexpected idField value of '%s'", idField)
				}
				if idValue != test.parent {
					t.Errorf("expected parent of '%s', got '%s'", test.parent, idValue)
				}
				defer func() { callCount++ }()
				if callCount > 0 {
					t.Errorf("expected QueryByID to be called once, but was called %d times", callCount+1)
				}
				cc := db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}
				err := test.queryByIDOutputErr
				return test.recordsToReturn, cc, err
			}
			client.batchDeleter = func(keys []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error) {
				if !reflect.DeepEqual(keys, test.keysToDelete) {
					t.Errorf("\nexpected:\n%+v\n\ngot:\n%+v\n", format(test.keysToDelete), format(keys))
				}
				return db.ConsumedCapacity{ConsumedCapacity: 1, ConsumedReadCapacity: 3, ConsumedWriteCapacity: 5}, test.deleteOutputErr
			}
			s := NewStoreWithClient(client)
			s.RegisterDataType(func() interface{} {
				return &testNodeData{}
			})
			s.RegisterDataType(func() interface{} {
				return &testEdgeData{}
			})
			err := s.DeleteEdge(test.parent, test.child)
			if err != test.expectedErr {
				t.Errorf("expected err %v, got %v", test.expectedErr, err)
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

func TestNewStore(t *testing.T) {
	s, err := NewStore("eu-west-2", "exampleTableName")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := s.Client.(*db.DB); !ok {
		t.Errorf("underlying default database has changed to %T, please check", s.Client)
	}
}
