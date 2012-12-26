crosstalk-worker-api-aws-dynamodb
=================================

`crosstalk-worker-api-aws-dynamodb` is a Crosstalk worker for interacting with AWS DynamoDB API

## Configuration

```json
{}
```

## crosstalk.on

[api.aws.dynamodb.batchWriteItem@v1](/crosstalk/crosstalk-worker-api-aws-dynamodb/wiki/api.aws.dynamodb.batchWriteItem@v1) - Batch write item

[api.aws.dynamodb.deleteItem@v1](/crosstalk/crosstalk-worker-api-aws-dynamodb/wiki/api.aws.dynamodb.deleteItem@v1) - Delete item

[api.aws.dynamodb.getItem@v1](/crosstalk/crosstalk-worker-api-aws-dynamodb/wiki/api.aws.dynamodb.getItem@v1) - Get item

[api.aws.dynamodb.putItem@v1](/crosstalk/crosstalk-worker-api-aws-dynamodb/wiki/api.aws.dynamodb.putItem@v1) - Put item

[api.aws.dynamodb.query@v1](/crosstalk/crosstalk-worker-api-aws-dynamodb/wiki/api.aws.dynamodb.query@v1) - Query

## crosstalk.emit

[~crosstalk.api.aws.signature.version4](/crosstalk/crosstalk-worker-api-aws-signature-version4/wiki/api.aws.signature.version4) - Requests an AWS Version 4 Signature to sign API requests with

## https out

`dynamodb.REGION.amazonaws.com` - For communicating with AWS DynamoDB API in specified `REGION`

