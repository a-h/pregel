package pregel

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/a-h/pregel/db"
	"github.com/a-h/pregel/rangefield"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// NewStore creates a store which is backed by DynamoDB.
func NewStore(region, tableName string) (store *Store, err error) {
	client, err := db.New(region, tableName)
	if err != nil {
		return nil, err
	}
	return NewStoreWithClient(client), nil
}

// NewStoreWithClient creates a store from a DB implementation.
func NewStoreWithClient(client DB) (store *Store) {
	store = &Store{
		Client:    client,
		DataTypes: make(map[string]func() interface{}),
	}
	return
}

// DB client to access DynamoDB.
type DB interface {
	BatchDelete(keys []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error)
	BatchPut(items []map[string]*dynamodb.AttributeValue) (db.ConsumedCapacity, error)
	QueryByID(idField, idValue string) (items []map[string]*dynamodb.AttributeValue, cc db.ConsumedCapacity, err error)
}

// Store handles storage of data in DynamoDB.
type Store struct {
	Client                DB
	ConsumedCapacity      float64
	ConsumedReadCapacity  float64
	ConsumedWriteCapacity float64
	DataTypes             map[string]func() interface{}
}

// RegisterDataType registers a data type.
func (s *Store) RegisterDataType(f func() interface{}) {
	v := f()
	s.DataTypes[getTypeName(v)] = f
}

func getTypeName(of interface{}) string {
	t := reflect.TypeOf(of)
	if t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	}
	return t.Name()
}

func convertToRecords(n Node) (records []map[string]*dynamodb.AttributeValue, err error) {
	records = append(records, newNodeRecord(n.ID))
	nodeDataRecords, err := convertNodeDataToRecords(n.ID, n.Data)
	if err != nil {
		return
	}
	records = append(records, nodeDataRecords...)
	edgeRecords, err := convertNodeEdgesToRecords(n.ID, n.Children, n.Parents)
	if err != nil {
		return
	}
	records = append(records, edgeRecords...)
	return
}

func convertNodeDataToRecords(id string, d Data) (nodeDataRecords []map[string]*dynamodb.AttributeValue, err error) {
	for k, v := range d {
		k := k
		v := v
		dr, dErr := newDataRecord(id, rangefield.NodeData{DataType: k}, k, v)
		if dErr != nil {
			err = dErr
			return
		}
		nodeDataRecords = append(nodeDataRecords, dr)
	}
	return
}

func convertNodeEdgesToRecords(id string, children []*Edge, parents []*Edge) (edgeRecords []map[string]*dynamodb.AttributeValue, err error) {
	// Add parent to child relationship.
	childRecords, err := convertEdgesToRecords(id, children, newChildRecord, newParentRecord)
	if err != nil {
		return
	}
	edgeRecords = append(edgeRecords, childRecords...)

	// Add child to parent relationship.
	for _, parent := range parents {
		parent := parent
		e := NewEdge(id)
		e.Data = parent.Data
		parentRecords, pErr := convertEdgesToRecords(parent.ID, []*Edge{e}, newParentRecord, newChildRecord)
		if pErr != nil {
			err = pErr
			return
		}
		edgeRecords = append(edgeRecords, parentRecords...)
	}
	return
}

func convertEdgesToRecords(principal string, edges []*Edge, fromPrincipal recordCreator, toPrincipal recordCreator) (edgeRecords []map[string]*dynamodb.AttributeValue, err error) {
	for _, e := range edges {
		e := e

		er, nErr := fromPrincipal(principal, e.ID, e.Data)
		if nErr != nil {
			err = nErr
			return
		}
		edgeRecords = append(edgeRecords, er...)

		er, nErr = toPrincipal(principal, e.ID, e.Data)
		if nErr != nil {
			err = nErr
			return
		}
		edgeRecords = append(edgeRecords, er...)
	}
	return
}

func (s *Store) updateCapacityStats(c db.ConsumedCapacity) {
	s.ConsumedCapacity += c.ConsumedCapacity
	s.ConsumedReadCapacity += c.ConsumedReadCapacity
	s.ConsumedWriteCapacity += c.ConsumedWriteCapacity
}

// Put upserts Nodes and Edges into DynamoDB.
func (s *Store) Put(nodes ...Node) (err error) {
	// Map from nodes into the Write Requests.
	var records []map[string]*dynamodb.AttributeValue
	for _, n := range nodes {
		if n.ID == "" {
			return ErrMissingNodeID
		}
		r, cErr := convertToRecords(n)
		if cErr != nil {
			err = cErr
			return
		}
		records = append(records, r...)
	}
	cc, err := s.Client.BatchPut(records)
	if err != nil {
		return
	}
	s.updateCapacityStats(cc)
	return
}

// PutNodeData into the store.
func (s *Store) PutNodeData(id string, data Data) (err error) {
	if id == "" {
		return ErrMissingNodeID
	}
	n := NewNode(id)
	n.Data = data
	return s.Put(n)
}

// PutEdges into the store.
func (s *Store) PutEdges(parent string, edges ...*Edge) (err error) {
	if parent == "" {
		return ErrMissingNodeID
	}
	records, err := convertNodeEdgesToRecords(parent, edges, nil)
	if err != nil {
		return
	}
	cc, err := s.Client.BatchPut(records)
	if err != nil {
		return
	}
	s.updateCapacityStats(cc)
	return
}

// PutEdgeData into the store.
func (s *Store) PutEdgeData(parent, child string, data Data) (err error) {
	if parent == "" || child == "" {
		return ErrMissingNodeID
	}
	e := NewEdge(child)
	e.Data = data
	return s.PutEdges(parent, e)
}

func getID(id string, rangeKey rangefield.RangeField) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		fieldID: {
			S: aws.String(id),
		},
		fieldRange: {
			S: aws.String(rangeKey.Encode()),
		},
	}
}

// ErrMissingNodeID is returned when a node's ID is empty.
var ErrMissingNodeID = errors.New("invalid node ID, IDs cannot be empty")

var errRecordIsMissingARangeField = errors.New("record is missing a range field")
var errRecordTypeFieldIsNil = errors.New("the record's range field is nil")

func errRecordTypeFieldUnknown(rt rangefield.RangeField) error {
	return fmt.Errorf("record type of '%T' is unknown", rt)
}

func errRecordTypeFieldUnhandled(rt rangefield.RangeField) error {
	return fmt.Errorf("record type of '%T' is not handled", rt)
}

func (s Store) populateNodeFromRecord(itm map[string]*dynamodb.AttributeValue, n *Node) error {
	tf, hasType := itm[fieldRange]
	if !hasType {
		return errRecordIsMissingARangeField
	}
	if tf.S == nil {
		return errRecordTypeFieldIsNil
	}
	f, ok := rangefield.Decode(*tf.S)
	if !ok {
		return errRecordTypeFieldUnknown(f)
	}
	switch rf := f.(type) {
	case rangefield.Node:
		n.ID = *itm[fieldID].S
		return nil
	case rangefield.NodeData:
		typeName := *itm[fieldRecordDataType].S
		f, ok := s.DataTypes[typeName]
		if !ok {
			f = func() interface{} { return &map[string]interface{}{} }
		}
		v := f()
		err := s.putData(itm, v)
		n.Data[typeName] = v
		return err
	case rangefield.Child:
		if e := n.GetChild(rf.Child); e == nil {
			n.Children = append(n.Children, NewEdge(rf.Child))
		}
		return nil
	case rangefield.ChildData:
		e := n.GetChild(rf.Child)
		if e == nil {
			e = NewEdge(rf.Child)
			n.Children = append(n.Children, e)
		}

		typeName := *itm[fieldRecordDataType].S
		f, ok := s.DataTypes[typeName]
		if !ok {
			f = func() interface{} { return &map[string]interface{}{} }
		}
		v := f()
		err := s.putData(itm, v)
		e.Data[typeName] = v
		return err
	case rangefield.Parent:
		if e := n.GetParent(rf.Parent); e == nil {
			n.Parents = append(n.Parents, NewEdge(rf.Parent))
		}
		return nil
	case rangefield.ParentData:
		e := n.GetParent(rf.Parent)
		if e == nil {
			e = NewEdge(rf.Parent)
			n.Parents = append(n.Parents, e)
		}

		typeName := *itm[fieldRecordDataType].S
		f, ok := s.DataTypes[typeName]
		if !ok {
			f = func() interface{} { return &map[string]interface{}{} }
		}
		v := f()
		err := s.putData(itm, v)
		e.Data[typeName] = v
		return err
	default:
		return errRecordTypeFieldUnhandled(rf)
	}
}

func (s Store) putData(itm map[string]*dynamodb.AttributeValue, into interface{}) (err error) {
	delete(itm, fieldID)
	delete(itm, fieldRange)
	delete(itm, fieldRecordDataType)
	err = dynamodbattribute.UnmarshalMap(itm, into)
	return
}

// Get retrieves data from DynamoDB.
func (s *Store) Get(id string) (n Node, ok bool, err error) {
	if id == "" {
		return
	}
	items, cc, err := s.Client.QueryByID(fieldID, id)
	if err != nil {
		err = fmt.Errorf("Store.Get: failed to query pages: %v", err)
		return
	}
	s.updateCapacityStats(cc)
	n = NewNode("")
	for _, itm := range items {
		err = s.populateNodeFromRecord(itm, &n)
		if err != nil {
			err = fmt.Errorf("Store.Get: failed to unmarshal data: %v", err)
			return
		}
	}
	ok = len(n.ID) > 0
	return
}

// Delete a node.
func (s *Store) Delete(id string) (err error) {
	// Get the IDs.
	n, ok, err := s.Get(id)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	keysToDelete := []map[string]*dynamodb.AttributeValue{
		getID(n.ID, rangefield.Node{}),
	}
	for dt := range n.Data {
		keysToDelete = append(keysToDelete,
			getID(n.ID, rangefield.NodeData{DataType: dt}))
	}
	for _, e := range n.Children {
		// Delete child and parent records.
		keysToDelete = append(keysToDelete,
			getID(n.ID, rangefield.Child{Child: e.ID}),
			getID(e.ID, rangefield.Parent{Parent: n.ID}))

		// Delete data records.
		for dataKey := range e.Data {
			keysToDelete = append(keysToDelete,
				getID(n.ID, rangefield.ChildData{Child: e.ID, DataType: dataKey}),
				getID(e.ID, rangefield.ParentData{Parent: n.ID, DataType: dataKey}))
		}
	}
	for _, e := range n.Parents {
		keysToDelete = append(keysToDelete,
			getID(n.ID, rangefield.Parent{Parent: e.ID}),
			getID(e.ID, rangefield.Child{Child: n.ID}))

		// Delete data records.
		for dataKey := range e.Data {
			keysToDelete = append(keysToDelete,
				getID(n.ID, rangefield.ParentData{Parent: e.ID, DataType: dataKey}),
				getID(e.ID, rangefield.ChildData{Child: n.ID, DataType: dataKey}))
		}
	}
	var cc db.ConsumedCapacity
	cc, err = s.Client.BatchDelete(keysToDelete)
	if err != nil {
		return
	}
	s.updateCapacityStats(cc)
	return
}

// DeleteEdge deletes an edge.
func (s *Store) DeleteEdge(parent string, child string) (err error) {
	// Get the IDs.
	n, ok, err := s.Get(parent)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	var keysToDelete []map[string]*dynamodb.AttributeValue
	for _, e := range n.Children {
		if e.ID != child {
			continue
		}
		// Delete child and parent records.
		keysToDelete = append(keysToDelete,
			getID(n.ID, rangefield.Child{Child: e.ID}),
			getID(e.ID, rangefield.Parent{Parent: n.ID}))

		// Delete data records.
		for dataKey := range e.Data {
			keysToDelete = append(keysToDelete,
				getID(n.ID, rangefield.ChildData{Child: e.ID, DataType: dataKey}),
				getID(e.ID, rangefield.ParentData{Parent: n.ID, DataType: dataKey}))
		}
	}
	var cc db.ConsumedCapacity
	cc, err = s.Client.BatchDelete(keysToDelete)
	if err != nil {
		return
	}
	s.updateCapacityStats(cc)
	return
}
