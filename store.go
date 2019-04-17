package pregel

import (
	"errors"
	"fmt"

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
}

func convertToRecords(n Node) (nodeRecord map[string]*dynamodb.AttributeValue, edgeRecords []map[string]*dynamodb.AttributeValue, err error) {
	nodeRecord, err = newNodeRecord(n.ID, n.Data)
	if err != nil {
		return
	}
	edgeRecords, err = convertNodeEdgesToRecords(n.ID, n.Children, n.Parents)
	return
}

func convertNodeEdgesToRecords(id string, children []Edge, parents []Edge) (edgeRecords []map[string]*dynamodb.AttributeValue, err error) {
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

func convertEdgesToRecords(principal string, edges []Edge, fromPrincipal recordCreator, toPrincipal recordCreator) (edgeRecords []map[string]*dynamodb.AttributeValue, err error) {
	for _, e := range edges {
		e := e

		er, nErr := fromPrincipal(principal, e.ID, e.Data)
		if nErr != nil {
			err = nErr
			return
		}
		edgeRecords = append(edgeRecords, er)

		er, nErr = toPrincipal(principal, e.ID, e.Data)
		if nErr != nil {
			err = nErr
			return
		}
		edgeRecords = append(edgeRecords, er)
	}
	return
}

func getWriteRequests(n Node) (requests []*dynamodb.WriteRequest, err error) {
	r, er, err := convertToRecords(n)
	if err != nil {
		return
	}
	// Add the root node.
	requests = append(requests, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: r,
		},
	})
	// Add the edges.
	for _, e := range er {
		e := e
		requests = append(requests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: e,
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

// PutEdges into the store.
func (s *Store) PutEdges(parent string, edges ...Edge) (err error) {
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

func getID(id string, t recordType, rangeKey string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		fieldID: {
			S: aws.String(id),
		},
		fieldRange: {
			S: aws.String(rangeField(t, rangeKey)),
		},
	}
}

var errRecordIsMissingARangeField = errors.New("record is missing a range field")
var errRecordTypeFieldIsNil = errors.New("the record's range field is nil")

func errRecordTypeFieldUnknown(rt recordType) error {
	return fmt.Errorf("record type of '%s' is unknown", rt)
}

func errRecordTypeFieldUnhandled(rt recordType) error {
	return fmt.Errorf("record type of '%s' is not handled", rt)
}

func populateNodeFromRecord(itm map[string]*dynamodb.AttributeValue, n *Node) error {
	tf, hasType := itm[fieldRange]
	if !hasType {
		return errRecordIsMissingARangeField
	}
	if tf.S == nil {
		return errRecordTypeFieldIsNil
	}
	rt, id, ok := rangeFieldSplit(*tf.S)
	if !ok {
		return errRecordTypeFieldUnknown(rt)
	}
	switch rt {
	case recordTypeNode:
		return populateNodeFromNodeRecord(itm, n)
	case recordTypeChild:
		e, err := convertEdgeRecordToEdge(itm, id)
		if err != nil {
			return err
		}
		n.Children = append(n.Children, e)
		return nil
	case recordTypeParent:
		e, err := convertEdgeRecordToEdge(itm, id)
		if err != nil {
			return err
		}
		n.Parents = append(n.Parents, e)
		return nil
	default:
		return errRecordTypeFieldUnhandled(rt)
	}
}

func populateNodeFromNodeRecord(itm map[string]*dynamodb.AttributeValue, n *Node) (err error) {
	n.ID = *itm[fieldID].S
	delete(itm, fieldID)
	delete(itm, fieldRange)
	err = dynamodbattribute.UnmarshalMap(itm, &n.Data)
	return
}

func convertEdgeRecordToEdge(itm map[string]*dynamodb.AttributeValue, id string) (e Edge, err error) {
	e.ID = id
	delete(itm, fieldID)
	delete(itm, fieldRange)
	err = dynamodbattribute.UnmarshalMap(itm, &e.Data)
	return
}

// Get retrieves data from DynamoDB.
func (s *Store) Get(id string) (n Node, ok bool, err error) {
	return s.GetWithTypedData(id, nil)
}

// GetWithTypedData gets data, but uses a pointer passed into the data parameter to populate the type
// of the data within the node.
func (s *Store) GetWithTypedData(id string, data interface{}) (n Node, ok bool, err error) {
	n.Data = data

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
			pageErr = populateNodeFromRecord(itm, &n)
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
				Key: getID(n.ID, recordTypeNode, nodeRecordChildConstant),
			},
		},
	}
	for _, e := range n.Children {
		deleteRequests = append(deleteRequests,
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(n.ID, recordTypeChild, e.ID),
				},
			},
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(e.ID, recordTypeParent, n.ID),
				},
			})
	}
	for _, e := range n.Parents {
		deleteRequests = append(deleteRequests,
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(n.ID, recordTypeParent, e.ID),
				},
			},
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: getID(e.ID, recordTypeChild, n.ID),
				},
			})
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
	deleteRequests := []*dynamodb.WriteRequest{
		&dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: getID(parent, recordTypeChild, child),
			},
		},
		&dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: getID(child, recordTypeParent, parent),
			},
		},
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
