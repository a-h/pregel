package pregel

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/a-h/pregel/rangefield"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// NewStore creates a new store with required fields populated.
func NewStore(region, tableName string) (store *Store, err error) {
	conf := &aws.Config{
		Region: aws.String(region),
	}
	sess, err := session.NewSession(conf)
	if err != nil {
		return
	}
	store = &Store{
		Client:    dynamodb.New(sess),
		TableName: aws.String(tableName),
		DataTypes: make(map[string]func() interface{}),
	}
	return
}

// Store handles storage of data in DynamoDB.
type Store struct {
	Client                *dynamodb.DynamoDB
	TableName             *string
	ConsumedCapacity      float64
	ConsumedReadCapacity  float64
	ConsumedWriteCapacity float64
	DataTypes             map[string]func() interface{}
}

// RegisterDataType registers a data type.
func (s *Store) RegisterDataType(f func() interface{}) {
	v := f()
	t := reflect.TypeOf(v)
	name := t.Name()
	if t.Kind() == reflect.Ptr {
		name = t.Elem().Name()
	}
	s.DataTypes[name] = f
}

func convertToRecords(n Node) (nodeRecord map[string]*dynamodb.AttributeValue,
	nodeDataRecords, edgeRecords, edgeDataRecords []map[string]*dynamodb.AttributeValue, err error) {
	nodeRecord = newNodeRecord(n.ID)
	nodeDataRecords, err = convertDataToRecords(n.ID, n.Data)
	if err != nil {
		return
	}
	edgeRecords, err = convertNodeEdgesToRecords(n.ID, n.Children, n.Parents)
	return
}

func convertDataToRecords(id string, d Data) (nodeDataRecords []map[string]*dynamodb.AttributeValue, err error) {
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
	parentRecords, err := convertEdgesToRecords(id, parents, newParentRecord, newChildRecord)
	if err != nil {
		return
	}
	edgeRecords = append(edgeRecords, parentRecords...)

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

func getWriteRequests(n Node) (requests []*dynamodb.WriteRequest, err error) {
	r, ndr, er, edr, err := convertToRecords(n)
	if err != nil {
		return
	}
	// Add the root node.
	requests = append(requests, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: r,
		},
	})
	// Add the node data.
	for _, nd := range ndr {
		nd := nd
		requests = append(requests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: nd,
			},
		})
	}
	// Add the edges.
	for _, e := range er {
		e := e
		requests = append(requests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: e,
			},
		})
	}
	// Add the edge data.
	for _, ed := range edr {
		ed := ed
		requests = append(requests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: ed,
			},
		})
	}
	return
}

func (s *Store) updateCapacityStats(c ...*dynamodb.ConsumedCapacity) {
	for _, cc := range c {
		if cc.CapacityUnits != nil {
			s.ConsumedCapacity += *cc.CapacityUnits
		}
		if cc.ReadCapacityUnits != nil {
			s.ConsumedReadCapacity += *cc.ReadCapacityUnits
		}
		if cc.WriteCapacityUnits != nil {
			s.ConsumedWriteCapacity += *cc.WriteCapacityUnits
		}
	}
}

// Put upserts Nodes and Edges into DynamoDB.
func (s *Store) Put(nodes ...Node) (err error) {
	// Map from nodes into the Write Requests.
	var wrs []*dynamodb.WriteRequest
	for _, n := range nodes {
		wrb, wrErr := getWriteRequests(n)
		if wrErr != nil {
			err = wrErr
			return
		}
		wrs = append(wrs, wrb...)
	}
	bwo, err := s.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*s.TableName: wrs,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	})
	if err != nil {
		return
	}
	s.updateCapacityStats(bwo.ConsumedCapacity...)
	return
}

// PutNodeData into the store.
func (s *Store) PutNodeData(id string, data Data) (err error) {
	//TODO: What happens if there isn't a node with that ID?
	records, err := convertDataToRecords(id, data)
	if err != nil {
		return
	}
	var wrs []*dynamodb.WriteRequest
	for _, r := range records {
		r := r
		wrs = append(wrs, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: r,
			},
		})
	}
	bwo, err := s.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*s.TableName: wrs,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	})
	if err != nil {
		return
	}
	s.updateCapacityStats(bwo.ConsumedCapacity...)
	return
}

// PutEdges into the store.
func (s *Store) PutEdges(parent string, edges ...*Edge) (err error) {
	records, err := convertNodeEdgesToRecords(parent, edges, nil)
	if err != nil {
		return
	}
	wrs := make([]*dynamodb.WriteRequest, len(records))
	for i := 0; i < len(records); i++ {
		wrs[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: records[i],
			},
		}
	}
	bwo, err := s.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*s.TableName: wrs,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	})
	if err != nil {
		return
	}
	s.updateCapacityStats(bwo.ConsumedCapacity...)
	return
}

// PutEdgeData into the store.
func (s *Store) PutEdgeData(parent, child string, data Data) (err error) {
	//TODO: What happens if there isn't a node with that ID, or a matching Edge?
	var wrs []*dynamodb.WriteRequest
	for k, v := range data {
		k := k
		v := v
		r, dErr := newDataRecord(parent, rangefield.ChildData{Child: child, DataType: k}, k, v)
		if err != nil {
			err = dErr
			return
		}
		wrs = append(wrs, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: r,
			},
		})
	}
	bwo, err := s.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*s.TableName: wrs,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	})
	if err != nil {
		return
	}
	s.updateCapacityStats(bwo.ConsumedCapacity...)
	return
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
	n = NewNode("")

	q := expression.Key(fieldID).Equal(expression.Value(id))

	expr, err := expression.NewBuilder().
		WithKeyCondition(q).
		Build()
	if err != nil {
		err = fmt.Errorf("Store.Get: failed to build query: %v", err)
		return
	}

	qi := &dynamodb.QueryInput{
		TableName:                 s.TableName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ConsistentRead:            aws.Bool(true),
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	}

	var pageErr error
	page := func(page *dynamodb.QueryOutput, lastPage bool) bool {
		for _, itm := range page.Items {
			pageErr = s.populateNodeFromRecord(itm, &n)
			if pageErr != nil {
				return false
			}
		}
		s.updateCapacityStats(page.ConsumedCapacity)
		return true
	}

	err = s.Client.QueryPages(qi, page)
	if err != nil {
		err = fmt.Errorf("Store.Get: failed to query pages: %v", err)
		return
	}
	if pageErr != nil {
		err = fmt.Errorf("Store.Get: failed to unmarshal data: %v", pageErr)
		return
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

	deleteRequests := []*dynamodb.WriteRequest{
		&dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: getID(n.ID, rangefield.Node{}),
			},
		},
		&dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: getID(n.ID, rangefield.NodeData{}),
			},
		},
	}
	for _, e := range n.Children {
		// Delete child and parent records.
		deleteRequests = append(deleteRequests,
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(n.ID, rangefield.Child{Child: e.ID}),
				},
			},
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(e.ID, rangefield.Parent{Parent: n.ID}),
				},
			})

		// Delete data records.
		for dataKey := range e.Data {
			deleteRequests = append(deleteRequests,
				&dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: getID(n.ID, rangefield.ChildData{Child: e.ID, DataType: dataKey}),
					},
				},
				&dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: getID(e.ID, rangefield.ParentData{Parent: n.ID, DataType: dataKey}),
					},
				})
		}
	}
	for _, e := range n.Parents {
		deleteRequests = append(deleteRequests,
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(n.ID, rangefield.Parent{Parent: e.ID}),
				},
			},
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(e.ID, rangefield.Child{Child: n.ID}),
				},
			})

		// Delete data records.
		for dataKey := range e.Data {
			deleteRequests = append(deleteRequests,
				&dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: getID(n.ID, rangefield.ParentData{Parent: e.ID, DataType: dataKey}),
					},
				},
				&dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: getID(e.ID, rangefield.ChildData{Child: n.ID, DataType: dataKey}),
					},
				})
		}
	}
	bwo, err := s.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*s.TableName: deleteRequests,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	})
	if err != nil {
		return
	}
	s.updateCapacityStats(bwo.ConsumedCapacity...)
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

	var deleteRequests []*dynamodb.WriteRequest
	for _, e := range n.Children {
		if e.ID != child {
			continue
		}
		// Delete child and parent records.
		deleteRequests = append(deleteRequests,
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(n.ID, rangefield.Child{Child: e.ID}),
				},
			},
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(e.ID, rangefield.Parent{Parent: n.ID}),
				},
			})

		// Delete data records.
		for dataKey := range e.Data {
			deleteRequests = append(deleteRequests,
				&dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: getID(n.ID, rangefield.ChildData{Child: e.ID, DataType: dataKey}),
					},
				},
				&dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: getID(e.ID, rangefield.ParentData{Parent: n.ID, DataType: dataKey}),
					},
				})
		}
	}
	bwo, err := s.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*s.TableName: deleteRequests,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	})
	if err != nil {
		return
	}
	s.updateCapacityStats(bwo.ConsumedCapacity...)
	return
}
