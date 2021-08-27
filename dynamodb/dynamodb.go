package dynamodb

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"log"
	"strings"
)

func CreateLocalClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	if err != nil {
		panic(err)
	}

	return dynamodb.NewFromConfig(cfg)
}

func tableExists(d *dynamodb.Client, name string) bool {
	tables, err := d.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		log.Fatal("ListTables failed", err)
	}
	for _, n := range tables.TableNames {
		if n == name {
			return true
		}
	}
	return false
}

func CreateTableIfNotExists(d *dynamodb.Client, tableName string) {
	if tableExists(d, tableName) {
		log.Printf("table=%v already exists\n", tableName)
		return
	}
	_, err := d.CreateTable(context.TODO(), buildCreateTableInput(tableName))
	if err != nil {
		log.Fatal("CreateTable failed", err)
	}
	log.Printf("created table=%v\n", tableName)
}

func buildCreateTableInput(tableName string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("PK"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("SK"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("PK"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("SK"),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
	}
}

func IsConditionCheckFailure(err error) bool {
	if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
		return true
	}
	var oe *smithy.OperationError
	if errors.As(err, &oe) {
		var re *http.ResponseError
		if errors.As(err, &re) {
			var tce *types.TransactionCanceledException
			if errors.As(err, &tce) {
				for _, reason := range tce.CancellationReasons {
					if *reason.Code == "ConditionalCheckFailed" {
						return true
					}
				}
			}
		}
	}

	return false
}

func DeleteAllItems(d *dynamodb.Client, tableName string) error {
	log.Printf("deleting all items from table=%v\n", tableName)
	var offset map[string]types.AttributeValue
	for {
		scanInput := &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		}
		if offset != nil {
			scanInput.ExclusiveStartKey = offset
		}
		result, err := d.Scan(context.TODO(), scanInput)
		if err != nil {
			return err
		}

		for _, item := range result.Items {
			_, err := d.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
				TableName: aws.String(tableName),
				Key:       map[string]types.AttributeValue{"PK": item["PK"], "SK": item["SK"]},
			},
			)
			if err != nil {
				return err
			}
		}

		if result.LastEvaluatedKey == nil {
			break
		}
		offset = result.LastEvaluatedKey
	}
	return nil

}
