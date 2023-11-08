import json
import boto3
import time
import random
import urllib.request
from datetime import datetime, timedelta, timezone

# 유저에게 post로 대분류, 중분류, 소분류, 캐릭터_datetime을 전달 받음


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
    # slack webhook url

    with urllib.request.urlopen(request) as response:
        slack_message = response.read()


def lambda_handler(event, context):
    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env = function_arn.split(":")[-1]

    dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")
    table = dynamodb.Table(f"Dy_story_event_{env}")
    dynamodb_client = boto3.client("dynamodb")

    # 시간
    datetime_utc = datetime.utcnow()
    timezone_kst = timezone(timedelta(hours=9))
    # 현재 한국 시간
    datetime_kst = datetime_utc.astimezone(timezone_kst)

    statusCode = 200
    try:
        httpMethod = event["httpMethod"]
    except:
        print("firebase user!!")

    try:
        httpMethod = event["routeKey"].split()[0]
    except:
        print("cognito user!!")

    if httpMethod != "POST":
        statusCode = 405
        return {"statusCode": statusCode, "body": json.dumps("Method Not Allowed")}
    # MVP 버전 분기
    version = "1"
    try:
        item = event["body"]
        item = json.loads(item)
        # MVP version2!
        if item["mode"]:
            version = "2"
    except:
        # 기존 version!
        version = "1"

    # 이전 상태 확인
    if version == "1":
        try:
            # major, middle, sub, character_datetime
            item = event["body"]
            item = json.loads(item)

            # cognito
            try:
                item["user_id"] = event["requestContext"]["authorizer"]["claims"][
                    "cognito:username"
                ]
            except:
                print("firebase user!!")

            # firebase
            try:
                item["user_id"] = event["requestContext"]["authorizer"]["jwt"][
                    "claims"
                ]["user_id"]
            except:
                print("cognito user!!")

            user_id = item["user_id"]
            # 동화 생성은 한번에 하나씩만
            query = f"select * from Dy_story_event_{env} where user_id='{item['user_id']}' and status='ongoing';"
            result = dynamodb_client.execute_statement(Statement=query)

            if len(result["Items"]):
                print("story fail!!")
                return {"statusCode": 400, "body": json.dumps("Bad Request")}

            # 현재 구현 X
            # 동화 생성이 가능한 상태 << 코인이 존재하는지 체크
            query = f"select * from Dy_user_{env} where user_id='{user_id}';"
            result = dynamodb_client.execute_statement(Statement=query)
            table_user_coin = dynamodb.Table(f"Dy_user_prod")

            if len(result["Items"]):
                coin = result["Items"][0]["coin"]["S"]
                if coin == "0":
                    print("no coin!")
                    return {"statusCode": 400, "body": json.dumps("Bad Request")}
                else:
                    coin = str(int(coin) - 1)
                    query = f"update Dy_user_{env} set coin='{coin}' where user_id='{user_id}';"
                    update_result = dynamodb_client.execute_statement(Statement=query)
                    print(f"current coin {user_id}, {coin}")
            else:
                # 기존에 존재하지 않는 유저
                coin_temp = {}
                coin_temp["user_id"] = user_id
                coin_temp["coin"] = "0"
                print(f"hello new user! {user_id}")
                temp = table_user_coin.put_item(Item=coin_temp)

        except:
            print("What error?")
            statusCode = 500
            return {
                "statusCode": statusCode,
                "body": json.dumps("Internal Server Error!"),
            }

        start_time = time.time()
        # put dynamodb!!
        try:
            # name은 공란 (향후 채워넣어야 함)
            # item['datetime']=datetime_kst.strftime('%Y-%m-%d-%H-%M-%S')
            item["time_stamp"] = str(int(datetime_kst.timestamp()))
            time_stamp = item["time_stamp"]
            item["status"] = "ongoing"
            # item['fail']="no"
            print(item)
            # get character_information!!!
            query = f"select age,gender,name from Dy_user_character_{env} where user_id='{item['user_id']}' and time_stamp='{item['character_datetime']}';"
            result = dynamodb_client.execute_statement(Statement=query)
            age = result["Items"][0]["age"]["N"]
            gender = result["Items"][0]["gender"]["S"]
            name = result["Items"][0]["name"]["S"]

            item["age"] = age
            item["gender"] = gender
            item["name"] = name
            item["title"] = "def"

            temp = table.put_item(Item=item)
        except:
            print("put dynamodb fail")
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}

        # put Dy_user_book (클라이언트가 책이 만들어지고 있음을 알기 위해)
        try:
            # Dy_user_book 추가
            # dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
            table = dynamodb.Table(f"Dy_user_book_{env}")
            item = {}
            # 존재한다면 S3에 책이 만들어진 상태! 덮어쓰기!
            item["user_id"] = user_id
            item["time_stamp"] = str(int(datetime_kst.timestamp()))
            item["title"] = ""
            item["status"] = "ongoing"
            item["num"] = str(random.randint(0, 5))

            # 초기 세팅은 공
            item["option"] = "public"

            temp = table.put_item(Item=item)

            try:
                # social db put
                table_social = dynamodb.Table(f"Dy_social_book_prod")
                tp = {}
                tp["user_id"] = user_id
                tp["time_stamp"] = time_stamp
                tp["like_num"] = 0
                print(user_id)
                print(time_stamp)
                tptp = table_social.put_item(Item=tp)
            except:
                print("???")

        except:
            print("put dynamodb fail")
            statusCode = 500
            return {
                "statusCode": statusCode,
                "body": json.dumps("Internet Server Error"),
            }
        end_time = time.time()
        print(f"dynamodb: {end_time - start_time:.5f}")

        start_time = time.time()
        # sqs 전송 (SQS_make_story)
        try:
            # 최종 처리는 sqs에 연결된 lambda가 진행
            sqs = boto3.resource("sqs", region_name="ap-northeast-2")
            queue = sqs.get_queue_by_name(QueueName=f"SQS_make_story_{env}")

            user_id = item["user_id"]
            time_stamp = item["time_stamp"]

            temp_json = {}
            temp_json["user_id"] = user_id
            temp_json["time_stamp"] = time_stamp
            temp_json["version"] = version
            message_body = json.dumps(temp_json)
            response = queue.send_message(
                MessageBody=message_body,
            )
        except ClientError as error:
            logger.exception("Send Upscale message failed: %s", message_body)
            raise error
        end_time = time.time()
        print(f"sqs: {end_time - start_time:.5f}")

        print("good")
        return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}

    # 신규 version MVP2!
    else:
        env = "prod"
        # 새로운 양식에 맞는 전처리 진행
        try:
            # mode, theme, background, character_type, character_datetime
            item = event["body"]
            item = json.loads(item)

            print(item)
            # cognito
            try:
                item["user_id"] = event["requestContext"]["authorizer"]["claims"][
                    "cognito:username"
                ]
            except:
                print("firebase user!!")

            # firebase
            try:
                item["user_id"] = event["requestContext"]["authorizer"]["jwt"][
                    "claims"
                ]["user_id"]
            except:
                print("cognito user!!")

            user_id = item["user_id"]
            # 동화 생성은 한번에 하나씩만
            query = f"select * from Dy_story_event_{env} where user_id='{item['user_id']}' and status='ongoing';"
            result = dynamodb_client.execute_statement(Statement=query)

            # ver이 global인지 확인!
            try:
                print("hi global!")
                if item["ver"]:
                    # global
                    pass
            except:
                item["ver"] = ""
                print("hi korean!")

            ver = item["ver"]

            if len(result["Items"]):
                print("story fail!!")
                return {"statusCode": 400, "body": json.dumps("Bad Request")}

            # 동화 생성이 가능한 상태 << 코인이 존재하는지 체크

            query = f"select * from Dy_user_{env} where user_id='{user_id}';"
            result = dynamodb_client.execute_statement(Statement=query)
            table_user_coin = dynamodb.Table(f"Dy_user_prod")

            if len(result["Items"]):
                coin = result["Items"][0]["coin"]["S"]
                if coin == "0":
                    print("no coin!")
                    return {"statusCode": 400, "body": json.dumps("Bad Request")}
                else:
                    coin = str(int(coin) - 1)
                    query = f"update Dy_user_{env} set coin='{coin}' where user_id='{user_id}';"
                    update_result = dynamodb_client.execute_statement(Statement=query)
                    print(f"current coin {user_id}, {coin}")
            else:
                # 기존에 존재하지 않는 유저
                coin_temp = {}
                coin_temp["user_id"] = user_id
                coin_temp["coin"] = "0"
                print(f"hello new user! {user_id}")
                temp = table_user_coin.put_item(Item=coin_temp)

            mode = item["mode"]
            theme = item["theme"]
            background = item["background"]
            character_type = item["character_type"]
            character_datetime = item["character_datetime"]

        except:
            print("What error?")
            statusCode = 500
            return {
                "statusCode": statusCode,
                "body": json.dumps("Internal Server Error!"),
            }

        # 유저 입력 동화
        if mode == "1":
            # 검증 단계 추가 해야함.
            pass

        # 주제 추천
        elif mode == "0":
            print(item["theme"]["major"])
            item["major"] = item["theme"]["major"]
            item["middle"] = item["theme"]["middle"]
            item["sub"] = item["theme"]["sub"]

            del item["theme"]
            item["theme"] = "0"

        # put dynamodb!!
        time_stamp = str(int(datetime_kst.timestamp()))
        try:
            # name은 공란 (향후 채워넣어야 함)
            # item['datetime']=datetime_kst.strftime('%Y-%m-%d-%H-%M-%S')
            item["time_stamp"] = time_stamp

            item["status"] = "ongoing"
            # item['fail']="no"
            print(item)

            # dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')

            # Dy_story_event_prod 작성
            table = dynamodb.Table(f"Dy_story_event_{env}")
            # dynamodb_client=boto3.client('dynamodb')

            # 유저가 캐릭터 선택
            if character_datetime != "0":
                # get character_information!!!
                query = f"select age,gender,name from Dy_user_character_{env} where user_id='{item['user_id']}' and time_stamp='{item['character_datetime']}';"
                result = dynamodb_client.execute_statement(Statement=query)
                age = result["Items"][0]["age"]["N"]
                gender = result["Items"][0]["gender"]["S"]
                name = result["Items"][0]["name"]["S"]

                item["age"] = str(age)
                item["gender"] = gender
                # item['name']=name
                item["title"] = "def"

            else:
                item["age"] = "6"
                if character_type == "1":
                    item["gender"] = "boy"
                    # item['name']='북이'
                else:
                    item["gender"] = "girl"
                    # item['name']='꼬미'
                item["title"] = "def"

            temp = table.put_item(Item=item)

            try:
                # social db put
                table_social = dynamodb.Table(f"Dy_social_book_prod")
                tp = {}
                tp["user_id"] = user_id
                tp["time_stamp"] = time_stamp
                tp["like_num"] = 0
                print(user_id)
                print(time_stamp)
                tptp = table_social.put_item(Item=tp)
            except:
                print("???")

        except:
            print("put dynamodb fail")
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}

        # put Dy_user_book (클라이언트가 책이 만들어지고 있음을 알기 위해)
        try:
            # Dy_user_book 추가
            # dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
            table = dynamodb.Table(f"Dy_user_book_{env}")
            item = {}
            # 존재한다면 S3에 책이 만들어진 상태! 덮어쓰기!
            item["user_id"] = user_id
            item["time_stamp"] = time_stamp
            item["title"] = ""
            item["status"] = "ongoing"
            item["num"] = str(random.randint(0, 5))
            item["ver"] = ver

            # 초기 동화 세팅은 public
            item["option"] = "public"

            temp = table.put_item(Item=item)

        except:
            print("put dynamodb fail")
            statusCode = 500
            return {
                "statusCode": statusCode,
                "body": json.dumps("Internet Server Error"),
            }

        # sqs 전송 (SQS_make_story)
        try:
            # 최종 처리는 sqs에 연결된 lambda가 진행
            sqs = boto3.resource("sqs", region_name="ap-northeast-2")
            queue = sqs.get_queue_by_name(QueueName=f"SQS_make_story_{env}")

            user_id = item["user_id"]
            time_stamp = item["time_stamp"]

            temp_json = {}
            temp_json["user_id"] = user_id
            temp_json["time_stamp"] = time_stamp
            temp_json["version"] = version
            temp_json["ver"] = ver
            message_body = json.dumps(temp_json)
            response = queue.send_message(
                MessageBody=message_body,
            )
        except ClientError as error:
            logger.exception("Send Upscale message failed: %s", message_body)
            raise error

        """
        # slack 노티
        try:
            post_slack(f"MVP version2 api test!! {user_id}, {time_stamp}")
        except:
            print("noti error!")
        """

        # 현재 ongoing 중인 책의 개수가 몇개인지 파악하면됨.
        query = f"select * from Dy_story_event_{env} where status='ongoing';"
        result = dynamodb_client.execute_statement(Statement=query)
        temp = {}
        temp["waiting_num"] = str(len(result["Items"]))
        jsonData = json.dumps(temp, ensure_ascii=False).encode("utf8")

        print("good vesion2")
        return {"statusCode": 200, "body": jsonData}
