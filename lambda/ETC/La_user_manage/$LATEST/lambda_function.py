import json
import boto3
import urllib.request


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

    # user_id를 통해 모든 dynamodb의 내용을 삭제
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

    # slack알림
    post_slack(f"from La_user_manage GET user delete request! user_id: {user_id}")
    # SQS에 직접 전송
    try:
        # 최종 처리는 sqs에 연결된 lambda가 진행
        sqs = boto3.resource("sqs", region_name="ap-northeast-2")
        queue = sqs.get_queue_by_name(QueueName=f"SQS_user_manage_{env}")
        temp_json = {}
        temp_json["user_id"] = user_id
        print("hi")
        message_body = json.dumps(temp_json)
        response = queue.send_message(
            MessageBody=message_body,
        )
    except:
        post_slack(f"from La_user_manage GET user delete request! user_id: {user_id}")

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
