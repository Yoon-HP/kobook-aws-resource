import json
import boto3
import time
import os
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from JsonImagine import JsonImagine

# dynamodb
dynamodb_client = boto3.client("dynamodb")


def lambda_handler(event, context):
    # event parsing (from sqs) user_id, time_stamp 전달 받음
    # print(event)
    try:
        # print(event)
        message_body = json.loads(event["Records"][0]["body"])
        # bucket_name=message_body['Records'][0]['s3']['bucket']['name']
        # object_key=message_body['Records'][0]['s3']['object']['key']

        user_id = message_body["user_id"]
        time_stamp = message_body["time_stamp"]

        query = f"SELECT character_datetime FROM Dy_story_event where user_id='{user_id}' and time_stamp='{time_stamp}'"
        print(query)
        result = dynamodb_client.execute_statement(Statement=query)
        print(result)
        print("check!!")

        character_datetime = result["Items"][0]["character_datetime"]["S"]
        print(character_datetime)

        # character datetime 먼저 구한 후 이를 바탕으로 이미지 url 가져오기
        query = f"SELECT img_url,style FROM Dy_user_character where user_id='{user_id}' and datetime='{character_datetime}'"
        result = dynamodb_client.execute_statement(Statement=query)
        print(result)
        print("check!!")
        img_url = result["Items"][0]["img_url"]["S"]
        style = result["Items"][0]["style"]["S"]

        # gpt prompt 가져오기 (from Dy_gpt_prompt)

        query = f"SELECT * FROM Dy_gpt_prompt where user_id='{user_id}' and time_stamp='{time_stamp}'"
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
            return {"statusCode": 200, "body": json.dumps("Hello")}

    except:
        print("event_parsing_fail!!!!!")
        return {
            "statusCode": 200,
            "body": json.dumps("Hello La_post_midjourney_character"),
        }

    # post midjourney!!!
    try:
        load_dotenv("key.env")
        MID_JOURNEY_ID = os.getenv("MID_JOURNEY_ID")
        SERVER_ID = os.getenv("SERVER_ID")
        CHANNEL_ID = os.getenv("CHANNEL_ID")
        header = {"authorization": os.getenv("VIP_TOKEN")}
        URL = "https://discord.com/api/v9/interactions"
        StorageURL = (
            "https://discord.com/api/v9/channels/" + CHANNEL_ID + "/attachments"
        )

        # 현재는 redirect 시키지 않고 디스코드 상의 url을 그대로 사용할 것임
        # my_redirect_url="my_redirect_url"
        # {my_redirect_url}/{object_key}

        # 페이지 수 만큼 요청을 보내야 함!

        for page_index in range(len(keys)):
            gpt_prompt = result["Items"][0][keys[page_index]]["S"]

            # style 통일!!! (고민)
            prompt = f"<#s_t/{user_id}/{time_stamp}/{page_index+1}> {img_url} {gpt_prompt}, {style} --turbo"
            print(prompt)
            __payload = JsonImagine(MID_JOURNEY_ID, SERVER_ID, CHANNEL_ID, prompt)
            # post to midjourney!!!
            response = requests.post(url=URL, json=__payload, headers=header)
            time.sleep(40)

    except:
        print("OTL...")

    print("Good!")
    return {"statusCode": 200, "body": json.dumps("Hello La_post_midjourney_story")}
