package db

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// New creates a new DynamoDB database tool.
func New(region, tableName string) (db *DB, err error) {
	conf := &aws.Config{
		Region: aws.String(region),
	}
	sess, err := session.NewSession(conf)
	if err != nil {
		return
	}
	db = &DB{
		Client:    dynamodb.New(sess),
		TableName: tableName,
	}
	return
}

// ConsumedCapacity from the DB.
type ConsumedCapacity struct {
	ConsumedCapacity      float64
	ConsumedReadCapacity  float64
	ConsumedWriteCapacity float64
}

func (c ConsumedCapacity) add(cc ConsumedCapacity) ConsumedCapacity {
	return ConsumedCapacity{
		ConsumedCapacity:      c.ConsumedCapacity + cc.ConsumedCapacity,
		ConsumedReadCapacity:  c.ConsumedReadCapacity + cc.ConsumedReadCapacity,
		ConsumedWriteCapacity: c.ConsumedWriteCapacity + cc.ConsumedWriteCapacity,
	}
}

func newConsumedCapacity(dcc ...*dynamodb.ConsumedCapacity) (cc ConsumedCapacity) {
	for _, itm := range dcc {
		if itm.CapacityUnits != nil {
			cc.ConsumedCapacity += *itm.CapacityUnits
		}
		if itm.ReadCapacityUnits != nil {
			cc.ConsumedReadCapacity += *itm.ReadCapacityUnits
		}
		if itm.WriteCapacityUnits != nil {
			cc.ConsumedWriteCapacity += *itm.WriteCapacityUnits
		}
	}
	return
}

// DB client for the store which uses DynamoDB.
type DB struct {
	Client    *dynamodb.DynamoDB
	TableName string
}

// BatchDelete items in the underlying table.
func (db *DB) BatchDelete(keys []map[string]*dynamodb.AttributeValue) (cc ConsumedCapacity, err error) {
	var deleteRequests []*dynamodb.WriteRequest
	for _, item := range keys {
		deleteRequests = append(deleteRequests,
			&dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: item,
				},
			})
	}
	bwo, err := db.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			db.TableName: deleteRequests,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	})
	if err != nil {
		return
	}
	cc = newConsumedCapacity(bwo.ConsumedCapacity...)
	return
}

// BatchPut items into the table.
func (db *DB) BatchPut(items []map[string]*dynamodb.AttributeValue) (cc ConsumedCapacity, err error) {
	var wrs []*dynamodb.WriteRequest
	for _, item := range items {
		wrs = append(wrs, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		})
	}
	bwo, err := db.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			db.TableName: wrs,
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	})
	if err != nil {
		return
	}
	cc = newConsumedCapacity(bwo.ConsumedCapacity...)
	return
}

// QueryByID returns items with a given ID field name and value.
func (db *DB) QueryByID(field, value string) (items []map[string]*dynamodb.AttributeValue, cc ConsumedCapacity, err error) {
	q := expression.Key(field).Equal(expression.Value(value))

	expr, err := expression.NewBuilder().
		WithKeyCondition(q).
		Build()
	if err != nil {
		err = fmt.Errorf("DB.QueryByID: failed to build query: %v", err)
		return
	}

	qi := &dynamodb.QueryInput{
		TableName:                 aws.String(db.TableName),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ConsistentRead:            aws.Bool(true),
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityIndexes),
	}

	var pageErr error
	page := func(page *dynamodb.QueryOutput, lastPage bool) bool {
		items = append(items, page.Items...)
		cc = cc.add(newConsumedCapacity(page.ConsumedCapacity))
		return true
	}

	err = db.Client.QueryPages(qi, page)
	if err != nil {
		err = fmt.Errorf("DB.QueryByID: failed to query pages: %v", err)
		return
	}
	if pageErr != nil {
		err = fmt.Errorf("DB.QueryByID: failed to unmarshal data: %v", pageErr)
		return
	}
	return
}
