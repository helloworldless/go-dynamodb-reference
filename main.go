package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dynamodbUtil "github.com/helloworldless/dynamodb-reference/dynamodb"
	"log"
	"math/rand"
	"strconv"
)

const tableName = "go-dynamodb-reference-table"

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile | log.LUTC)
	log.Println("starting")
	port := 8000
	log.Printf("creating DynaomDB client for DynamoDB Local running on port %d\n", port)
	dynamodbClient := dynamodbUtil.CreateLocalClient(port)
	log.Printf("creating table '%s' if it does not already exist\n", tableName)
	didCreateTable := dynamodbUtil.CreateTableIfNotExists(dynamodbClient, tableName)
	log.Printf("did create table '%s'? %v\n", tableName, didCreateTable)
	log.Println("running conditional check failure example")
	putItemConditionCheckFailureExample(dynamodbClient)
	log.Println("running seed items example")
	seedItems(dynamodbClient)
	log.Println("running delete all items example")
	deleteAllItems(dynamodbClient)
	log.Println("completed")
}

// Performs a PutItem with the same item twice. The second time fails with a conditional check failure.
func putItemConditionCheckFailureExample(dynamodbClient *dynamodb.Client) {
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

	log.Printf("putting item %v\n", item)
	err = putItem(dynamodbClient, tableName, ddbJson)
	if err != nil {
		log.Fatal("PutItem failed", err)
	}

	log.Println("putting same item; this should fail with condition check failure")
	err = putItem(dynamodbClient, tableName, ddbJson)
	if err == nil {
		log.Fatal("expected duplicate PutItem request to fail with condition check failure, but it did not")
	}

	if dynamodbUtil.IsConditionalCheckFailure(err) {
		log.Println("as expected: condition check failure error", err)
	} else {
		log.Println("unexpected error", err)
	}
}

func seedItems(dynamodbClient *dynamodb.Client) {
	dynamodbUtil.CreateTableIfNotExists(dynamodbClient, tableName)
	for i := 0; i < 500; i++ {
		item := map[string]types.AttributeValue{
			"PK":            &types.AttributeValueMemberS{Value: "PK-" + strconv.Itoa(i)},
			"SK":            &types.AttributeValueMemberS{Value: "A"},
			"RandomContent": &types.AttributeValueMemberS{Value: createRandomTextContent(10000)},
		}
		err := putItem(dynamodbClient, tableName, item)
		if err != nil {
			log.Fatal("failed to put item", err)
		}
	}
}

func createRandomTextContent(n int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func putItem(d *dynamodb.Client, tableName string, item map[string]types.AttributeValue) error {
	_, err := d.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	return err
}

func deleteAllItems(dynamodbClient *dynamodb.Client) {
	err := dynamodbUtil.DeleteAllItems(dynamodbClient, tableName)
	if err != nil {
		log.Fatal("failed to delete all items", err)
	}
}
