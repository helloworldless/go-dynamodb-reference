package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ddb "github.com/helloworldless/dynamodb-reference/dynamodb"
	"log"
	"math/rand"
	"strconv"
)

const tableName = "go-dynamodb-reference-table"

func main() {
	log.Println("starting")
	conditionCheckFailure()
	putItemsAndDeleteAll()
	log.Println("completed")
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func putItemsAndDeleteAll() {
	d := ddb.CreateLocalClient()
	ddb.CreateTableIfNotExists(d, tableName)
	for i := 0; i < 500; i++ {
		item := map[string]types.AttributeValue{
			"PK":     &types.AttributeValueMemberS{Value: "PK-" + strconv.Itoa(i)},
			"SK":     &types.AttributeValueMemberS{Value: "A"},
			"Filler": &types.AttributeValueMemberS{Value: RandStringRunes(10000)},
		}
		err := putItem(d, tableName, item)
		if err != nil {
			log.Fatal("failed to put item", err)
		}
	}

	err := ddb.DeleteAllItems(d, tableName)
	if err != nil {
		log.Fatal("failed to delete all items", err)
	}

	scan, err := d.Scan(context.TODO(), &dynamodb.ScanInput{TableName: aws.String(tableName)})
	if err != nil {
		log.Fatal("scan failed", err)
	}
	log.Printf("expected scan to have zero items; it had len=%d\n", len(scan.Items))
}

func conditionCheckFailure() {
	d := ddb.CreateLocalClient()
	ddb.CreateTableIfNotExists(d, tableName)
	err := ddb.DeleteAllItems(d, tableName)
	if err != nil {
		log.Fatal("failed to delete all items", err)
	}
	item := struct {
		PK string `dynamodbav:"PK"`
		SK string `dynamodbav:"SK"`
	}{
		PK: "ITEM#123",
		SK: "A",
	}
	ddbJson, err := attributevalue.MarshalMap(item)
	if err != nil {
		log.Fatal("failed to marshal item", err)
	}

	log.Println("putting item")
	err = putItem(d, tableName, ddbJson)
	if err != nil {
		log.Fatal("PutItem failed", err)
	}

	log.Println("putting same item, should fail with condition check failure")
	err = putItem(d, tableName, ddbJson)
	if err != nil {
		log.Fatal("PutItem failed", err)
	}

	if ddb.IsConditionCheckFailure(err) {
		log.Println("condition check failure error", err)
	} else {
		log.Println("general error", err)
	}
}

func putItem(d *dynamodb.Client, tableName string, item map[string]types.AttributeValue) error {
	_, err := d.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	return err
}
