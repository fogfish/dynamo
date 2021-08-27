#!/bin/sh

aws dynamodb create-table \
  --table-name example-dynamo-relational \
  --attribute-definitions \
    AttributeName=prefix,AttributeType=S \
    AttributeName=suffix,AttributeType=S \
    AttributeName=publisher,AttributeType=S \
    AttributeName=year,AttributeType=S \
  --key-schema \
    AttributeName=prefix,KeyType=HASH \
    AttributeName=suffix,KeyType=RANGE \
  --local-secondary-indexes \
    '[{ "IndexName": "example-dynamo-relational-year", "KeySchema": [{"AttributeName": "prefix", "KeyType": "HASH"}, {"AttributeName": "year", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"} }]' \
  --global-secondary-indexes \
    '[{ "IndexName": "example-dynamo-relational-category-year", "KeySchema": [{"AttributeName": "category", "KeyType": "HASH"}, {"AttributeName": "year", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}, "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5} }]' \
  --provisioned-throughput \
    ReadCapacityUnits=5,WriteCapacityUnits=5
