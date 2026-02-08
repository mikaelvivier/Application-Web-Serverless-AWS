import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")
table = dynamodb.Table("dynamodb-all-messages")


def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }


def get_messages(body):
    channel_id = body.get("channel_id")
    last_evaluated_key = body.get("last_evaluated_key")

    if not channel_id:
        return build_response(400, {"error": "Missing channel_id"})

    try:
        query_params = {
            "KeyConditionExpression": Key("channel_id").eq(channel_id),
            "Limit": 10,
            "ScanIndexForward": False
        }

        if last_evaluated_key:
            query_params["ExclusiveStartKey"] = last_evaluated_key

        response = table.query(**query_params)

        return build_response(200, {
            "items": response.get("Items", []),
            "last_evaluated_key": response.get("LastEvaluatedKey")
        })
    except ClientError as e:
        print("DynamoDB error:", e)
        return build_response(400, {"error": e.response["Error"]["Message"]})


def post_message(body):
    author = body.get("author")
    content = body.get("content")
    channel_id = body.get("channel_id")

    if not author or not content or not channel_id:
        return build_response(400, {"error": "Missing author, content, or channel_id"})

    timestamp = datetime.utcnow().isoformat()
    item = {
        "channel_id": channel_id,
        "timestamp_utc_iso8601": timestamp,
        "author": author,
        "content": content
    }

    try:
        table.put_item(Item=item)
        return build_response(201, item)
    except ClientError as e:
        print("DynamoDB error:", e)
        return build_response(400, {"error": e.response["Error"]["Message"]})


def lambda_handler(event, context):
    print("Request event:", event)

    http_method = event.get("httpMethod")
    path = event.get("path")
    body = {}

    if event.get("body"):
        try:
            body = json.loads(event["body"])
        except json.JSONDecodeError:
            return build_response(400, {"error": "Invalid JSON"})

    if http_method == "POST" and path == "/Getmessages":
        return get_messages(body)
    elif http_method == "POST" and path == "/messages":
        return post_message(body)
    else:
        return build_response(404, {"error": "Not found"})
