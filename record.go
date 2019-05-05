package pregel

import (
	"github.com/a-h/pregel/rangefield"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	fieldID             = "id"
	fieldRange          = "rng"
	fieldRecordDataType = "t"
)

func newNodeRecord(id string) (r map[string]*dynamodb.AttributeValue) {
	return newRecord(id, rangefield.Node{})
}

type recordCreator func(from, to string, data Data) (r []map[string]*dynamodb.AttributeValue, err error)

func newChildRecord(parent, child string, data Data) (r []map[string]*dynamodb.AttributeValue, err error) {
	r = append(r, newRecord(parent, rangefield.Child{Child: child}))
	for k, v := range data {
		k := k
		v := v
		dr, dErr := newDataRecord(parent, rangefield.ChildData{Child: child, DataType: k}, k, v)
		if dErr != nil {
			err = dErr
			return
		}
		r = append(r, dr)
	}
	return
}

func newParentRecord(parent, child string, data Data) (r []map[string]*dynamodb.AttributeValue, err error) {
	r = append(r, newRecord(child, rangefield.Parent{Parent: parent}))
	for k, v := range data {
		k := k
		v := v
		dr, dErr := newDataRecord(child, rangefield.ParentData{Parent: parent, DataType: k}, k, v)
		if dErr != nil {
			err = dErr
			return
		}
		r = append(r, dr)
	}
	return
}

func newRecord(id string, rangeKey rangefield.RangeField) (r map[string]*dynamodb.AttributeValue) {
	r = make(map[string]*dynamodb.AttributeValue)
	r[fieldID] = &dynamodb.AttributeValue{S: &id}
	r[fieldRange] = &dynamodb.AttributeValue{S: aws.String(rangeKey.Encode())}
	return
}

func newDataRecord(id string, rangeKey rangefield.RangeField, key string, value interface{}) (r map[string]*dynamodb.AttributeValue, err error) {
	r, err = dynamodbattribute.MarshalMap(value)
	if err != nil {
		return
	}
	r[fieldID] = &dynamodb.AttributeValue{S: &id}
	r[fieldRange] = &dynamodb.AttributeValue{S: aws.String(rangeKey.Encode())}
	r[fieldRecordDataType] = &dynamodb.AttributeValue{S: aws.String(key)}
	return
}
