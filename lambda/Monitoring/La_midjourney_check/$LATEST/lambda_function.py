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

    """
    # Dy_midjourney_check_prod 기록
    datetime_utc = datetime.utcnow()
    timezone_kst = timezone(timedelta(hours=9))
    # 현재 한국 시간
    datetime_kst = datetime_utc.astimezone(timezone_kst)
    temp=str(datetime_kst.timestamp())
    temp=temp.split(".")
    temp=temp[0]+temp[1][:3]
    time_sort_key=temp
    print(time_sort_key)
    """
    # parsing and check
    try:
        pk = event["pk"]
        time_sort_key = event["time"]
        prompt = event["prompt"]

        # 최대 5초 안에는 discord에 들어갔어야 함
        dynamodb_client = boto3.client("dynamodb")
        time.sleep(70)

        # 나에게만 보기로 생성되는 경우가 많기 때문에, 해당 로직은 예외가 많음
        """
        for _ in range(5):
            query=f"SELECT \"check\" from Dy_midjourney_check_{env} where pk='{pk}'"
            result=dynamodb_client.execute_statement(Statement=query)
            check=result["Items"][0]['check']['S']
            if check=="no":
                time.sleep(1)
            else:
                # discord 상에 들어감 < 무한 waiting 관련 문제 확인 필요
                # 평균 시간 계산 필요
                time.sleep(35)
                try:
                    # 최대 35초 이내엔 생성이 완료 되야함. << 이 기간에 생성되지 않았다면, waiting 관련 이슈가 발생했거나 원본 메시지가 삭제된 것! -> 관리자 노티
                    dynamodb_client=boto3.client('dynamodb')
                    for _ in range(5):
                        query=f"SELECT \"check\" from Dy_midjourney_check_{env} where pk='{pk}'"
                        result=dynamodb_client.execute_statement(Statement=query)
                        check=result["Items"][0]['check']['S']
                        if check!="end":
                            time.sleep(1)
                        else:
                            # midjourney 작업 완료!
                            print("job finished!")
                            return {
                                'statusCode': 200,
                                'body': json.dumps('Hello from Lambda!')
                            }
                    # appeal 이슈가 발생함
                    # post_slack(f"aws character waiting issue or deleted issue occur!! {pk}, {prompt}")
                except:
                    print("something went wrong!!!")
                
                print("good")
                return {
                    'statusCode': 200,
                    'body': json.dumps('Hello from Lambda!')
                }
        """
        query = f"SELECT \"check\" from Dy_midjourney_check_{env} where pk='{pk}'"
        result = dynamodb_client.execute_statement(Statement=query)
        check = result["Items"][0]["check"]["S"]

        # 미드저니 버전 변경 혹은 무한 wating issue가 발생한 것
        if check == "no":
            post_slack(f"aws 무한 웨이팅 혹은 버전 변경 occur!! {pk}, {prompt}")
        elif check == "yes":
            # 무한 웨이팅
            post_slack(f"aws 무한 웨이팅 occur!! {pk}, {prompt}")
        else:
            # 문제가 없는 상황
            print("good~")
            return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}

        # appeal 이슈가 발생함

        """
        # Dy_appeal_prompt_prod put
        try:
            # dynamodb
            dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
            table=dynamodb.Table(f"Dy_appeal_prompt_{env}")
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

        # yes 혹은 no 일 때는 향후 서비스에 문제가 없도록 처리
        query = f"update Dy_midjourney_check_{env} set \"check\"='check_before' where pk='{pk}'"
        result = dynamodb_client.execute_statement(Statement=query)

    except:
        print("parsing!!!")

    # 무한 waiting 및 원본 메시지 삭제 check

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
