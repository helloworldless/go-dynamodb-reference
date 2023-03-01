# DynamoDB Reference for Go

A few example using the [AWS SDK for Go V2](https://aws.github.io/aws-sdk-go-v2/docs/).

## Steps to Run

Make sure [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) is
running:

```shell
docker run -p 8000:8000 amazon/dynamodb-local
```

Install dependencies:

```shell
go get ./...
```

Run the program:

```shell
go run main.go
```
