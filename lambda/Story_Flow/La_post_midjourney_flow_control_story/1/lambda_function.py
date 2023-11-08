import json
import boto3
import time
from datetime import datetime, timedelta, timezone

# dynamodb에 생성되어야 하는 page에 대한 정보 저장 및 처음 trigger


def lambda_handler(event, context):
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env = function_arn.split(":")[-1]
    print(env)
    # mode 0: 처음 생성 요청 1: 재생성 2: panout
    mode = None

    # From sqs (SQS_post_midjourney_story) - user_id, time_stamp, character_datetime parsing
    try:
        mode = 0
        # print(event)
        message_body = json.loads(event["Records"][0]["body"])
        # bucket_name=message_body['Records'][0]['s3']['bucket']['name']
        # object_key=message_body['Records'][0]['s3']['object']['key']

        user_id = message_body["user_id"]
        time_stamp = message_body["time_stamp"]
        print(user_id)
        print(time_stamp)

        dynamodb_client = boto3.client("dynamodb")
        try:
            query = f"SELECT character_datetime FROM Dy_story_event_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            print(query)
            result = dynamodb_client.execute_statement(Statement=query)
            print(result)
            character_datetime = result["Items"][0]["character_datetime"]["S"]
            print(character_datetime)
        except:
            print("query fail!")

        # 디폴트 캐릭터 전용 flow 개발 해야함.

        # character datetime 먼저 구한 후 이를 바탕으로 이미지 url 가져오기 (8page에 대한 정보)
        dynamodb_client = boto3.client("dynamodb")
        query = f"SELECT img_url,style, object_key FROM Dy_user_character_{env} where user_id='{user_id}' and time_stamp='{character_datetime}'"
        result = dynamodb_client.execute_statement(Statement=query)
        print(result)
        print("check!!")
        img_url = result["Items"][0]["img_url"]["S"]
        style = result["Items"][0]["style"]["S"]
        try:
            object_key = result["Items"][0]["object_key"]["S"]
        except:
            object_key = ""
            print("why?")

        # gpt prompt 가져오기 (from Dy_gpt_prompt)

        query = f"SELECT * FROM Dy_gpt_prompt_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
        result = dynamodb_client.execute_statement(Statement=query)
        print(result)
        print("check!!")

        try:
            temp = list(result["Items"][0].keys())
            temp.remove("user_id")
            temp.remove("time_stamp")
            temp.sort()
            keys = temp
        except:
            print("parsing fail!")

    except:
        # print("event_parsing_fail!!!!!")
        print("hello backoffice!")
        # From lambda (La_backoffice_post)
        try:
            # mode=(1 or 2)
            print(event)
            user_id = event["user_id"]
            time_stamp = event["time_stamp"]
            in_dex = event["in_dex"]
            prompt_update = event["prompt_update"]
            sort_key = event["sort_key"]
            mode = event["mode"]
            try:
                character_check = event["character_check"]
                # print(character_check)
                # print(type(character_check))
            except:
                print("character_check check fail")

            # in_dex에 해당하는 img_url이 필요한 상황

            dynamodb_client = boto3.client("dynamodb")
            try:
                query = f"SELECT character_datetime FROM Dy_story_event_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result = dynamodb_client.execute_statement(Statement=query)
                character_datetime = result["Items"][0]["character_datetime"]["S"]
                print(character_datetime)

                # character datetime 먼저 구한 후 이를 바탕으로 이미지 url 가져오기 (8page에 대한 정보)
                dynamodb_client = boto3.client("dynamodb")
                query = f"SELECT img_url,style,object_key FROM Dy_user_character_{env} where user_id='{user_id}' and time_stamp='{character_datetime}'"
                result = dynamodb_client.execute_statement(Statement=query)
                print(result)
                print("check!!")
                img_url = result["Items"][0]["img_url"]["S"]
                style = result["Items"][0]["style"]["S"]
                try:
                    object_key = result["Items"][0]["object_key"]["S"]
                    print(object_key)
                except:
                    object_key = ""
                    print("why?")
            except:
                print("query fail!")

            # update 된 prompt로 작업
            my_redirect_url = "my_redirect_url"
            pk = f"s_t/{user_id}/{time_stamp}/{in_dex}/{mode}"
            if character_check:
                if object_key:
                    prompt = f"<#s_t/{user_id}/{time_stamp}/{in_dex}/{mode}> {my_redirect_url}/{object_key}, {prompt_update}, {style}"
                else:
                    prompt = f"<#s_t/{user_id}/{time_stamp}/{in_dex}/{mode}> {img_url}, {prompt_update}, {style}"
            else:
                prompt = f"<#s_t/{user_id}/{time_stamp}/{in_dex}/{mode}> {prompt_update}, {style}"
            print(prompt)

            try:
                # dynamodb
                dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")
                table = dynamodb.Table(f"Dy_midjourney_check_story_{env}")

                message_body_dy = {}
                message_body_dy["pk"] = pk
                message_body_dy["in_dex"] = in_dex
                message_body_dy["prompt"] = prompt
                message_body_dy["check"] = "start"
                message_body_dy["time"] = ""
                # 처음 그림 생성
                message_body_dy["mode"] = mode
                # dynamodb put!
                temp = table.put_item(Item=message_body_dy)
            except:
                print("dynamodb put fail!!")

            # 현재 디스코드 채널 상의 job 개수를 고려해 lambda에 전달
            try:
                query = f"SELECT * from Dy_midjourney_check_story_{env} where \"check\"='yes' or \"check\"='no' and mode='{mode}'"
                result = dynamodb_client.execute_statement(Statement=query)
                # print(result)

                # 현재 디스코드 채널 상에서 작업중인 job의 개수
                job_number = len(result["Items"])
                print(job_number)
            except:
                print("dynamodb query fail!!")

            try:
                query = f"SELECT * from Dy_midjourney_check_story_{env} where \"check\"='start' and mode='{mode}'"
                result = dynamodb_client.execute_statement(Statement=query)
                print(result)
                # 시간순으로 오래된 요청부터 처리 및 낮은 in_dex부터 생성
                time_sort = []
                for item in result["Items"]:
                    pk = item["pk"]["S"]
                    in_dex = pk.split("/")[3]
                    time_stamp = pk.split("/")[2]
                    time_sort.append([time_stamp, in_dex, pk])
                time_sort.sort()
                print(time_sort)

                if (5 - job_number) > 0 and time_sort:
                    # 시간지연으로 인한 꼬임 방지
                    try:
                        # update
                        dynamodb_client = boto3.client("dynamodb")
                        query = f"UPDATE Dy_midjourney_check_story_{env} SET \"check\" = 'no' WHERE pk='{time_sort[0][2]}' and mode='{mode}';"
                        result = dynamodb_client.execute_statement(Statement=query)
                    except:
                        print("dynamodb update fail!!")

                    # midjourney post lambda invoke!
                    try:
                        lambda_client = boto3.client("lambda")
                        payload = {"pk": time_sort[0][2], "mode": mode}
                        response = lambda_client.invoke(
                            FunctionName="La_post_midjourney_story:prod",
                            InvocationType="Event",
                            Payload=json.dumps(payload),
                        )
                    except:
                        print("lambda invoke fail!")
            except:
                print("something went wrong!!")

        except:
            print("parsing fail!")

        return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}

    # 재생성과 panout에 대한 처리 job 별도로 처리 - DB 처리를 어떻게 가져갈지 고민할 필요가 있음
    # story 생성 요청 기록 -> Dy_midjourney_check_story_prod
    if mode == 0:
        # page 수 만큼 요청 기록
        for page_index in range(len(keys)):
            gpt_prompt = result["Items"][0][keys[page_index]]["S"]
            pk = f"s_t/{user_id}/{time_stamp}/{page_index+1}/{mode}"

            # 주인공 등장
            my_redirect_url = "my_redirect_url"
            if page_index % 2 == 0:
                # style 통일!!! (고민) turbo 사용 X
                if object_key:
                    prompt = f"<#s_t/{user_id}/{time_stamp}/{page_index+1}/{mode}> {my_redirect_url}/{object_key}, {gpt_prompt}, {style} --iw .5"
                else:
                    prompt = f"<#s_t/{user_id}/{time_stamp}/{page_index+1}/{mode}> {img_url}, {gpt_prompt}, {style} --iw .5"
                print(prompt)

            # 배경만
            else:
                if object_key:
                    prompt = f"<#s_t/{user_id}/{time_stamp}/{page_index+1}/{mode}> {my_redirect_url}/{object_key}, {gpt_prompt}, {style} --iw .5"
                else:
                    prompt = f"<#s_t/{user_id}/{time_stamp}/{page_index+1}/{mode}> {img_url}, {gpt_prompt}, {style} --iw .5"
                print(prompt)

            try:
                # dynamodb
                dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")
                table = dynamodb.Table(f"Dy_midjourney_check_story_{env}")

                message_body_dy = {}
                message_body_dy["pk"] = pk
                message_body_dy["in_dex"] = str(page_index + 1)
                message_body_dy["prompt"] = prompt
                message_body_dy["check"] = "start"
                message_body_dy["time"] = ""
                # 처음 그림 생성
                message_body_dy["mode"] = "0"
                # dynamodb put!
                temp = table.put_item(Item=message_body_dy)
            except:
                print("dynamodb put fail!!")

        # lambda invoke process - La_post_midjourney_story

        # 현재 디스코드 채널 상의 job 개수를 고려해 lambda에 전달
        try:
            query = f"SELECT * from Dy_midjourney_check_story_{env} where \"check\"='yes' or \"check\"='no' and mode='0'"
            result = dynamodb_client.execute_statement(Statement=query)
            # print(result)

            # 현재 디스코드 채널 상에서 작업중인 job의 개수
            job_number = len(result["Items"])
            print(job_number)
        except:
            print("dynamodb query fail!!")

        try:
            query = f"SELECT * from Dy_midjourney_check_story_{env} where \"check\"='start' and mode='0'"
            result = dynamodb_client.execute_statement(Statement=query)
            print(result)
            # 시간순으로 오래된 요청부터 처리 및 낮은 in_dex부터 생성
            time_sort = []
            for item in result["Items"]:
                pk = item["pk"]["S"]
                in_dex = pk.split("/")[3]
                time_stamp = pk.split("/")[2]
                time_sort.append([time_stamp, in_dex, pk])
            time_sort.sort()
            print(time_sort)

            if (5 - job_number) > 0 and time_sort:
                # 시간지연으로 인한 꼬임 방지
                try:
                    # update
                    dynamodb_client = boto3.client("dynamodb")
                    query = f"UPDATE Dy_midjourney_check_story_{env} SET \"check\" = 'no' WHERE pk='{time_sort[0][2]}' and mode='0';"
                    result = dynamodb_client.execute_statement(Statement=query)
                except:
                    print("dynamodb update fail!!")

                # midjourney post lambda invoke!
                try:
                    lambda_client = boto3.client("lambda")
                    payload = {"pk": time_sort[0][2], "mode": mode}
                    response = lambda_client.invoke(
                        FunctionName="La_post_midjourney_story:prod",
                        InvocationType="Event",
                        Payload=json.dumps(payload),
                    )
                except:
                    print("lambda invoke fail!")
        except:
            print("something went wrong!!")

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
