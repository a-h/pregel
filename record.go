package pregel

import (
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	fieldID             = "id"
	fieldRange          = "rng"
	fieldRecordDataType = "t"
)

type recordType string

const (
	recordTypeNode   recordType = "node"
	recordTypeChild  recordType = "child"
	recordTypeParent recordType = "parent"
)

func rangeField(t recordType, value string) string {
	if t == recordTypeNode {
		return string(recordTypeNode)
	}
	return string(t) + "/" + value
}

func rangeFieldSplit(field string) (rt recordType, value string, ok bool) {
	if field == string(recordTypeNode) {
		rt = recordTypeNode
		ok = true
		return
	}
	res := strings.SplitN(field, "/", 2)
	rt = recordType(res[0])
	ok = rt == recordTypeNode || rt == recordTypeParent || rt == recordTypeChild
	value = res[1]
	return
}

// The record structure here uses ID as the hash key and Child as the range key.
const nodeRecordChildConstant = ""

func newNodeRecord(id string, data interface{}) (r map[string]*dynamodb.AttributeValue, err error) {
	return newRecord(id, rangeField(recordTypeNode, nodeRecordChildConstant), data)
}

type recordCreator func(from, to string, data interface{}) (r map[string]*dynamodb.AttributeValue, err error)

func newChildRecord(parent, child string, data interface{}) (r map[string]*dynamodb.AttributeValue, err error) {
	return newRecord(parent, rangeField(recordTypeChild, child), data)
}

func newParentRecord(parent, child string, data interface{}) (r map[string]*dynamodb.AttributeValue, err error) {
	return newRecord(child, rangeField(recordTypeParent, parent), data)
}

func newRecord(id, rangeKey string, data interface{}) (r map[string]*dynamodb.AttributeValue, err error) {
	r, err = dynamodbattribute.MarshalMap(data)
	if err != nil {
		return
	}
	r[fieldID] = &dynamodb.AttributeValue{S: &id}
	if data != nil {
		r[fieldRecordDataType] = &dynamodb.AttributeValue{S: aws.String(reflect.TypeOf(data).Name())}
	}
	r[fieldRange] = &dynamodb.AttributeValue{S: &rangeKey}
	return
}
