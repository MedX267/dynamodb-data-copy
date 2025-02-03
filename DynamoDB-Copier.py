#!/usr/bin/env python
import boto3
import sys
import os
import time
from botocore.exceptions import ClientError

if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} <source_table_name> <destination_table_name>")
    sys.exit(1)

src_table = sys.argv[1]
dst_table = sys.argv[2]
region = os.getenv('AWS_DEFAULT_REGION', 'us-west-2')

dynamodb = boto3.resource('dynamodb', region_name=region)
client = boto3.client('dynamodb', region_name=region)

try:
    src_table_obj = dynamodb.Table(src_table)
    src_table_obj.load()
except ClientError:
    print(f"Error: Source table '{src_table}' does not exist or cannot be accessed.")
    sys.exit(1)

table_info = client.describe_table(TableName=src_table)['Table']
hash_key = next(attr['AttributeName'] for attr in table_info['KeySchema'] if attr['KeyType'] == 'HASH')
range_key = next((attr['AttributeName'] for attr in table_info['KeySchema'] if attr['KeyType'] == 'RANGE'), None)

try:
    dst_table_obj = dynamodb.Table(dst_table)
    dst_table_obj.load()
    print(f"Table {dst_table} already exists. Skipping creation.")
except ClientError as e:
    if e.response['Error']['Code'] == 'ResourceNotFoundException':
        key_schema = [{'AttributeName': hash_key, 'KeyType': 'HASH'}]
        attribute_definitions = [{'AttributeName': hash_key, 'AttributeType': 'S'}]

        if range_key:
            key_schema.append({'AttributeName': range_key, 'KeyType': 'RANGE'})
            attribute_definitions.append({'AttributeName': range_key, 'AttributeType': 'S'})

        dst_table_obj = dynamodb.create_table(
            TableName=dst_table,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode='PAY_PER_REQUEST'
        )

        dst_table_obj.wait_until_exists()
    else:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if 'DISABLE_DATACOPY' in os.environ:
    sys.exit(0)

scan_kwargs = {}
while True:
    response = src_table_obj.scan(**scan_kwargs)
    items = response.get('Items', [])
    
    with dst_table_obj.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

    if 'LastEvaluatedKey' not in response:
        break
    scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']

print("Data copy completed successfully.")
