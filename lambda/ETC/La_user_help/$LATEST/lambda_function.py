import json
import boto3
import urllib.request
import time
from datetime import datetime, timedelta, timezone


def post_slack(argStr):
    message = argStr
    send_data = {
        "text": message,
    }
    send_text = json.dumps(send_data)
    request = urllib.request.Request(
        "slack webhook url",
        data=send_text.encode("utf-8"),
    )

    with urllib.request.urlopen(request) as response:
        slack_message = response.read()


def lambda_handler(event, context):
    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env = function_arn.split(":")[-1]
    print(env)

    dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")
    dynamodb_client = boto3.client("dynamodb")
    datetime_utc = datetime.utcnow()
    timezone_kst = timezone(timedelta(hours=9))
    # 현재 한국 시간
    datetime_kst = datetime_utc.astimezone(timezone_kst)

    # cognito
    try:
        user_id = event["requestContext"]["authorizer"]["claims"]["cognito:username"]
    except:
        print("firebase user!!")

    # firebase
    try:
        user_id = event["requestContext"]["authorizer"]["jwt"]["claims"]["user_id"]
    except:
        print("cognito user!!")

    # parsing 작업
    try:
        # post 요청 관련해서 firebase user 추가 작업 필
        item = event["body"]
        item = json.loads(item)
        qna = item["qna"]
    except:
        print("parsing fail!!")

    # put dynamodb
    try:
        table = dynamodb.Table(f"Dy_user_help_{env}")
        # name은 공란 (향후 채워넣어야 함)
        # item['datetime']=datetime_kst.strftime('%Y-%m-%d-%H-%M-%S')
        item["time_stamp"] = str(int(datetime_kst.timestamp()))
        item["status"] = "before"
        item["user_id"] = user_id

        temp = table.put_item(Item=item)

    except:
        print("put dynamodb fail")
        statusCode = 400
        return {"statusCode": statusCode, "body": json.dumps("Bad Request")}

    # slack알림
    post_slack(f"고객의 소리!!! user_id:{user_id}, qna:{qna}")

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
