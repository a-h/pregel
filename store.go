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
	Client    *dynamodb.DynamoDB
	TableName *string
}

func convertToRecords(n Node) (nodeRecord map[string]*dynamodb.AttributeValue, edgeRecords []map[string]*dynamodb.AttributeValue, err error) {
	nodeRecord, err = newNodeRecord(n.ID, n.Data)
	if err != nil {
		return
	}
	edgeRecords, err = convertEdgesToRecords(n.ID, n.Edges)
	if err != nil {
		return
	}
	return
}

func convertEdgesToRecords(parentID string, edges []Edge) (edgeRecords []map[string]*dynamodb.AttributeValue, err error) {
	for _, e := range edges {
		e := e
		er, nErr := newEdgeRecord(parentID, e.ID, e.Data)
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

// Put upserts Nodes and Edges into DynamoDB.
func (rs *Store) Put(nodes ...Node) (err error) {
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
	_, err = rs.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*rs.TableName: wrs,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	})
	return
}

// PutEdges into the store.
func (rs *Store) PutEdges(parent string, edges ...Edge) (err error) {
	wrs := make([]*dynamodb.WriteRequest, len(edges))
	records, err := convertEdgesToRecords(parent, edges)
	if err != nil {
		return
	}
	for i := 0; i < len(edges); i++ {
		wrs[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: records[i],
			},
		}
	}
	_, err = rs.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*rs.TableName: wrs,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	})
	return
}

func getID(id string, child string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"id": {
			S: aws.String(id),
		},
		"child": {
			S: aws.String(child),
		},
	}
}

var errRecordIsMissingATypeField = errors.New("record is missing an 'rt' (record type) field")
var errRecordTypeFieldIsNil = errors.New("the record's 'rt' (record type) field is nil")

func errRecordTypeFieldUnknown(t string) error {
	return fmt.Errorf("record 'rt' (record type) field of '%s' is unknown", t)
}

func populateNodeFromRecord(itm map[string]*dynamodb.AttributeValue, n *Node) error {
	t, hasType := itm[fieldRecordType]
	if !hasType {
		return errRecordIsMissingATypeField
	}
	if t.S == nil {
		return errRecordTypeFieldIsNil
	}
	switch *t.S {
	case string(recordTypeNode):
		return populateNodeFromNodeRecord(itm, n)
	case string(recordTypeEdge):
		return populateNodeFromEdgeRecord(itm, n)
	}
	return errRecordTypeFieldUnknown(*t.S)
}

func populateNodeFromNodeRecord(itm map[string]*dynamodb.AttributeValue, n *Node) error {
	n.ID = *itm[fieldID].S
	// Trim the standard fields.
	delete(itm, fieldID)
	delete(itm, fieldChild)
	delete(itm, fieldRecordType)
	err := dynamodbattribute.UnmarshalMap(itm, &n.Data)
	if err != nil {
		return err
	}
	return nil
}

func populateNodeFromEdgeRecord(itm map[string]*dynamodb.AttributeValue, n *Node) error {
	var e Edge
	e.ID = *itm[fieldChild].S
	// Trim the standard fields.
	delete(itm, fieldID)
	delete(itm, fieldChild)
	delete(itm, fieldRecordType)
	// Populate any special type.
	err := dynamodbattribute.UnmarshalMap(itm, &e.Data)
	if err != nil {
		return err
	}
	n.Edges = append(n.Edges, e)
	return nil
}

// GetParentsOf the child.
func (rs *Store) GetParentsOf(child string) (parents []string, err error) {
	q := expression.
		Key(fieldChild).
		Equal(expression.Value(child))

	expr, err := expression.NewBuilder().
		WithKeyCondition(q).
		Build()
	if err != nil {
		err = fmt.Errorf("Store.GetParentsOf: failed to build query: %v", err)
		return
	}

	qi := &dynamodb.QueryInput{
		IndexName:                 aws.String("parentsOfChild"),
		TableName:                 rs.TableName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	//TODO: Worry about how many parents there could be?
	page := func(page *dynamodb.QueryOutput, lastPage bool) bool {
		for _, itm := range page.Items {
			v := *itm["id"].S
			if v == nodeRecordChildConstant {
				continue
			}
			parents = append(parents, v)
		}
		return true
	}

	err = rs.Client.QueryPages(qi, page)
	if err != nil {
		err = fmt.Errorf("Store.GetParentsOf: failed to query pages: %v", err)
		return
	}

	return
}

// Get retrieves data from DynamoDB.
func (rs *Store) Get(id string) (n Node, ok bool, err error) {
	return rs.GetWithTypedData(id, nil)
}

// GetWithTypedData gets data, but uses a pointer passed into the data parameter to populate the type
// of the data within the node.
func (rs *Store) GetWithTypedData(id string, data interface{}) (n Node, ok bool, err error) {
	n.Data = data

	q := expression.Key("id").Equal(expression.Value(id))

	expr, err := expression.NewBuilder().
		WithKeyCondition(q).
		Build()
	if err != nil {
		err = fmt.Errorf("Store.Get: failed to build query: %v", err)
		return
	}

	qi := &dynamodb.QueryInput{
		TableName:                 rs.TableName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ConsistentRead:            aws.Bool(true),
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	var pageErr error
	page := func(page *dynamodb.QueryOutput, lastPage bool) bool {
		for _, itm := range page.Items {
			pageErr = populateNodeFromRecord(itm, &n)
			if pageErr != nil {
				return false
			}
		}
		return true
	}

	err = rs.Client.QueryPages(qi, page)
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
func (rs *Store) Delete(id string) (err error) {
	// Get the IDs.
	n, ok, err := rs.Get(id)
	if err != nil {
		return
	}
	if !ok {
		return
	}
	deleteRequests := make([]*dynamodb.WriteRequest, len(n.Edges)+1)
	deleteRequests[0] = &dynamodb.WriteRequest{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: getID(n.ID, nodeRecordChildConstant),
		},
	}
	for i, e := range n.Edges {
		deleteRequests[i+1] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: getID(n.ID, e.ID),
			},
		}
	}
	_, err = rs.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*rs.TableName: deleteRequests,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	})
	//TODO: Use the consumed capacity.
	//TODO: Delete parent edges.
	return
}

// DeleteEdge deletes an edge.
func (rs *Store) DeleteEdge(parent string, child string) (err error) {
	id := getID(parent, child)
	// Also use the range key.
	id["child"], err = dynamodbattribute.Marshal(child)
	if err != nil {
		return
	}
	d := &dynamodb.DeleteItemInput{
		TableName: rs.TableName,
		Key:       id,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	_, err = rs.Client.DeleteItem(d)
	return
}
