import json
import time
import urllib.request
import boto3
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
    # parsing and check
    try:
        pk = event["pk"]
        prompt = event["prompt"]
        mode = event["mode"]

        time.sleep(5)
        # 최대 5초 안에는 discord에 들어갔어야 함
        dynamodb_client = boto3.client("dynamodb")
        # True라면 appeal 발생
        flag = True
        for _ in range(5):
            query = f"SELECT \"check\" from Dy_midjourney_check_{env} where pk='{pk}' and mode='{mode}'"
            result = dynamodb_client.execute_statement(Statement=query)
            check = result["Items"][0]["check"]["S"]
            if check == "no":
                time.sleep(1)
            else:
                # discord 상에 들어감
                print("good")
                flag = False
                brea
                """
                return {
                    'statusCode': 200,
                    'body': json.dumps('Hello from Lambda!')
                }
                """
        if flag:
            # appeal 이슈가 발생함
            post_slack(f"aws train_data appeal occur!! {pk}, {prompt}")
            # Dy_appeal_prompt_prod put
            """
            try:
                # dynamodb
                dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
                table=dynamodb.Table(f"Dy_appeal_prompt_prod")
                message_body_dy={}
                message_body_dy['pk']=pk
                message_body_dy['prompt']=prompt
                # dynamodb put!
                temp=table.put_item(
                    Item=message_body_dy
                )
            except:
                print("dynamodb put fail!!")
            """
    except:
        print("parsing!!!")

    # 무한 waiting 및 원본 메시지 삭제 check! -> 관리자 노티

    # 평균 시간 계산 필요 fast로 생성할 것.
    time.sleep(600)
    try:
        # 최대 35초 이내엔 생성이 완료 되야함. << 이 기간에 생성되지 않았다면, waiting 관련 이슈가 발생했거나 원본 메시지가 삭제된 것! -> 관리자 노티
        dynamodb_client = boto3.client("dynamodb")
        for _ in range(5):
            query = f"SELECT \"check\" from Dy_midjourney_check_{env} where pk='{pk}' and mode='{mode}'"
            result = dynamodb_client.execute_statement(Statement=query)
            check = result["Items"][0]["check"]["S"]
            if check != "end":
                time.sleep(1)
            else:
                # midjourney 작업 완료!
                print("job finished!")
                return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
        # appeal 이슈가 발생함
        post_slack(
            f"aws train_data waiting issue or deleted issue occur!! {pk}, {prompt}"
        )
    except:
        print("something went wrong!!!")

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
