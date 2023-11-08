import json
import boto3
import time
import os
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from JsonImagine import JsonImagine

# style prompt 필요!!!

# dynamodb
dynamodb_client = boto3.client("dynamodb")


def lambda_handler(event, context):
    # event parsing (from sqs)
    # print(event)
    try:
        # print(event)
        message_body = json.loads(event["Records"][0]["body"])
        # bucket_name=message_body['Records'][0]['s3']['bucket']['name']
        # object_key=message_body['Records'][0]['s3']['object']['key']
        object_key = message_body["key"]
        # object_key만 있으면 됨
        print(object_key)
        user_id = object_key.split("/")[1]
        datetime = object_key.split("/")[2].split(".")[0]
        print("check!!")
        query = f"SELECT gender,style,age FROM Dy_character_event where user_id='{user_id}' and datetime='{datetime}'"
        result = dynamodb_client.execute_statement(Statement=query)
        print(result)
        print("check!!")

        # 다시 N으로 바꿔줘야함
        age = str(result["Items"][0]["age"]["N"])
        gender = result["Items"][0]["gender"]["S"]
        style = result["Items"][0]["style"]["S"]

    except:
        print("event_parsing_fail!!!!!")

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

        # 임시 도메인... (팀 도메인 필요)
        my_redirect_url = "my_redirect_url"
        # {my_redirect_url}/{object_key}

        if style == "2D":
            prompt = f"<#{object_key}> {my_redirect_url}/{object_key} a {age} year {gender}, asian, cute, smiling, full body, main character, emotional, surreal, vibrant, Anime isekai --turbo"
        elif style == "Pixar":
            prompt = f"<#{object_key}> {my_redirect_url}/{object_key} a {age} year {gender}, asian, cute, smiling, full body, main character, emotional, surreal, vibrant, Pixar::2 --iw 0.6 --turbo"
        elif style == "Studio Ghibli":
            prompt = f"<#{object_key}> {my_redirect_url}/{object_key} a {age} year {gender}, cute, smiling, full body, main character, emotional, surreal, Studio Ghibli::2, by Miyazaki --iw 0.6 --turbo"
        else:
            prompt = f"<#{object_key}> {my_redirect_url}/{object_key} a {age} year {gender}, style by {style} --turbo"

        __payload = JsonImagine(MID_JOURNEY_ID, SERVER_ID, CHANNEL_ID, prompt)
        # post to midjourney!!!
        response = requests.post(url=URL, json=__payload, headers=header)
    except:
        print("OTL...")

    print("Good!")
    return {"statusCode": 200, "body": json.dumps("Hello La_post_midjourney_character")}
