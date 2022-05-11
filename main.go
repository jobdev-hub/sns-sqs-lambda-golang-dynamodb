package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	events "github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Record struct {
	ID   string `json:"id"`
	NAME string `json:"name"`
	AGE  int    `json:"age"`
}

var dyn *dynamodb.DynamoDB
var tbl, region string

func init() {

	tbl = os.Getenv("TABLE_NAME")
	region = os.Getenv("REGION")

	dyn = dynamodb.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	})))
}

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, sqsEvent events.SQSEvent) (err error) {
	for _, message := range sqsEvent.Records {
		err = putEvent(message)
		if err != nil {
			return err
		}
	}
	return err
}

func putEvent(message events.SQSMessage) error {

	var record Record
	err := json.Unmarshal([]byte(message.Body), &record)
	if err != nil {
		return err
	}

	// go get github.com/google/uuid
	// record.ID = uuid.New().String()

	input := &dynamodb.PutItemInput{
		TableName: aws.String(tbl),
		Item: map[string]*dynamodb.AttributeValue{
			"id":   {S: aws.String(record.ID)},
			"name": {S: aws.String(record.NAME)},
			"age":  {N: aws.String(fmt.Sprintf("%d", record.AGE))},
		},
	}

	_, err = dyn.PutItem(input)
	if err != nil {
		log.Fatal(err)
	}

	return err
}
