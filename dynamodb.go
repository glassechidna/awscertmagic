package awscertmagic

import (
	"cirello.io/dynamolock"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/mholt/certmagic"
	"time"
)

type DynamoDb struct {
	api   dynamodbiface.DynamoDBAPI
	table string
	locker dynamolock.Client
}

type item struct {
	PKey     string `dynamodbav:"PartitionKey"`
	Key      string `dynamodbav:"SortKey"`
	Value    []byte
	Modified time.Time
}

func NewDynamoDb(api dynamodbiface.DynamoDBAPI, table string) *DynamoDb {
	return &DynamoDb{
		api: api,
		table: table,
	}
}

func (d *DynamoDb) Lock(key string) error {
	return nil // TODO
}

func (d *DynamoDb) Unlock(key string) error {
	return nil // TODO
}

func (d *DynamoDb) Store(key string, value []byte) error {
	item := item{PKey: "pk", Key: key, Value: value, Modified: time.Now()}
	ddbItem, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{TableName: &d.table, Item: ddbItem}
	_, err = d.api.PutItem(input)
	return err
}

func (d *DynamoDb) Load(key string) ([]byte, error) {
	item, err := d.get(key)
	if err != nil {
		return nil, err
	}

	return item.Value, nil
}

func (d *DynamoDb) Delete(key string) error {
	input := &dynamodb.DeleteItemInput{TableName: &d.table, Key: ddbKey(key)}
	_, err := d.api.DeleteItem(input)
	return err
}

func (d *DynamoDb) Exists(key string) bool {
	_, err := d.get(key)
	return err == nil
}

func (d *DynamoDb) List(prefix string, recursive bool) ([]string, error) {
	output := []string{}

	err := d.api.QueryPages(&dynamodb.QueryInput{
		TableName:              &d.table,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("PartitionKey = :PartitionKey AND begins_with(SortKey, :SortKeyPrefix)"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":PartitionKey":  {S: aws.String("pk")},
			":SortKeyPrefix": {S: &prefix},
		},
	}, func(page *dynamodb.QueryOutput, lastPage bool) bool {
		items := []item{}
		_ = dynamodbattribute.UnmarshalListOfMaps(page.Items, &items)
		for _, item := range items {
			output = append(output, item.Key)
		}
		return !lastPage
	})

	return output, err
}

func (d *DynamoDb) Stat(key string) (certmagic.KeyInfo, error) {
	item, err := d.get(key)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	return certmagic.KeyInfo{Key: key, Modified: item.Modified}, nil
}

func (d *DynamoDb) get(key string) (*item, error) {
	output, err := d.api.GetItem(&dynamodb.GetItemInput{
		TableName:      &d.table,
		ConsistentRead: aws.Bool(true),
		Key:            ddbKey(key),
	})
	if err != nil {
		return nil, err
	} else if output.Item == nil {
		return nil, errors.New("not found")
	}

	item := item{}
	err = dynamodbattribute.UnmarshalMap(output.Item, &item)
	if err != nil {
		return nil, err
	}

	return &item, nil
}

func ddbKey(key string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"PartitionKey": {S: aws.String("pk")},
		"SortKey":      {S: &key},
	}
}
