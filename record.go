package pregel

import (
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	fieldID             = "id"
	fieldRecordType     = "rt"
	fieldRecordDataType = "t"
	fieldChild          = "child"
)

type recordType string

const (
	recordTypeNode recordType = "node"
	recordTypeEdge recordType = "edge"
)

// The record structure here uses ID as the hash key and Child as the range key.
const nodeRecordChildConstant = "_"

func newNodeRecord(id string, data interface{}) (r map[string]*dynamodb.AttributeValue, err error) {
	r, err = dynamodbattribute.MarshalMap(data)
	if err != nil {
		return
	}
	r[fieldID] = &dynamodb.AttributeValue{
		S: &id,
	}
	r[fieldRecordType] = &dynamodb.AttributeValue{S: aws.String(string(recordTypeNode))}
	if data != nil {
		n := reflect.TypeOf(data).Name()
		r[fieldRecordDataType] = &dynamodb.AttributeValue{S: aws.String(n)}
	}
	r[fieldChild] = &dynamodb.AttributeValue{S: aws.String(nodeRecordChildConstant)}
	return
}

func newEdgeRecord(parent, child string, data interface{}) (r map[string]*dynamodb.AttributeValue, err error) {
	r, err = dynamodbattribute.MarshalMap(data)
	if err != nil {
		return
	}
	r[fieldID] = &dynamodb.AttributeValue{S: &parent}
	r[fieldRecordType] = &dynamodb.AttributeValue{S: aws.String(string(recordTypeEdge))}
	if data != nil {
		n := reflect.TypeOf(data).Name()
		r[fieldRecordDataType] = &dynamodb.AttributeValue{S: aws.String(n)}
	}
	r[fieldChild] = &dynamodb.AttributeValue{S: aws.String(child)}
	return
}
