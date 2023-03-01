# DynamoDB Reference for Go

Make sure [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) is running:

```shell
docker run -p 8000:8000 amazon/dynamodb-local
```

Run the program:

```shell
go get ./...
go run main.go
```
