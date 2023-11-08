import json
import boto3
import time
import requests
import os
from PIL import Image
from datetime import datetime, timedelta, timezone
from presigned_url import generate_presigned_url


def lambda_handler(event, context):
    # TODO implement
    print(event)

    # from s3-kkobook-processing
    env = "prod"
    pk = event["Records"][0]["s3"]["object"]["key"]

    user_id = pk.split(".")[1]
    time_stamp = pk.split(".")[2]
    in_dex = pk.split(".")[3]
    mode = pk.split(".")[4]
    print(user_id, time_stamp, in_dex, mode)

    # s3
    # 사진 가져오기
    s3 = boto3.client("s3")
    s3.download_file("s3-kkobook-processing", f"{pk}", "/tmp/original_image.png")

    img = Image.open("/tmp/original_image.png")

    width, height = img.size
    left = 0
    top = 0
    right = width // 2
    bottom = height // 2

    # 이미지 4분할
    img1 = img.crop((left, top, right, bottom))
    img2 = img.crop((right, top, width, bottom))
    img3 = img.crop((left, bottom, right, height))
    img4 = img.crop((right, bottom, width, height))

    print(img1.size)
    # 분할된 이미지 저장

    img1 = img1.resize((1024, 1024))
    img2 = img2.resize((1024, 1024))
    img3 = img3.resize((1024, 1024))
    img4 = img4.resize((1024, 1024))

    img1.save("/tmp/image1.png")
    img2.save("/tmp/image2.png")
    img3.save("/tmp/image3.png")
    img4.save("/tmp/image4.png")

    s3.upload_file(
        "/tmp/image1.png",
        "s3-kkobook-story-image",
        f"upscale/{user_id}/{time_stamp}/{in_dex}/1.jpg",
    )
    s3.upload_file(
        "/tmp/image2.png",
        "s3-kkobook-story-image",
        f"upscale/{user_id}/{time_stamp}/{in_dex}/2.jpg",
    )
    s3.upload_file(
        "/tmp/image3.png",
        "s3-kkobook-story-image",
        f"upscale/{user_id}/{time_stamp}/{in_dex}/3.jpg",
    )
    s3.upload_file(
        "/tmp/image4.png",
        "s3-kkobook-story-image",
        f"upscale/{user_id}/{time_stamp}/{in_dex}/4.jpg",
    )

    datetime_utc = datetime.utcnow()
    timezone_kst = timezone(timedelta(hours=9))
    # 현재 한국 시간
    datetime_kst = datetime_utc.astimezone(timezone_kst)

    temp = str(datetime_kst.timestamp())
    temp = temp.split(".")
    temp = temp[0] + temp[1][:3]
    time_sort_key = temp

    dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")
    dynamodb_client = boto3.client("dynamodb")

    dy_temp = {}
    dy_temp["user_id"] = user_id
    dy_temp["time_stamp"] = time_stamp
    dy_temp["time"] = time_sort_key
    dy_temp["in_dex"] = in_dex

    table = dynamodb.Table(f"Dy_midjourney_output_story_prod")

    temp = table.put_item(Item=dy_temp)
    time.sleep(1)

    # check db update

    try:
        temp_pk = f"s_t/{user_id}/{time_stamp}/{in_dex}/{mode}"
        query_temp = f"update Dy_midjourney_check_story_prod set \"check\"='recovery_fin' where pk='{temp_pk}' and mode='0';"
        print(query_temp)
        result_ck = dynamodb_client.execute_statement(Statement=query_temp)
    except:
        print("check plz ")
        print(temp_pk)

    # 만약 동화의 페이지가 모두 생성되었다면 다음 작업 진행 << version2 임을 알 수 있어야 함.
    query = f"select in_dex from Dy_midjourney_output_story_prod where user_id='{user_id}' and time_stamp='{time_stamp}';"
    result_ck = dynamodb_client.execute_statement(Statement=query)
    print(result_ck)

    temp = []
    flag = True
    try:
        for ck in result_ck["Items"]:
            temp.append(ck["in_dex"]["S"])

        for i in range(1, 9):
            if str(i) not in temp:
                flag = False
    except:
        print("fail!")

    print(flag)

    # 모든 책의 작업이 완료된 상황! << 꼬일 수 있는 경우가 존재해 모든 페이지가 생성되었는지 판단하는 로직 필요
    if flag:
        print("Hi~")

        # Dy_social_book_prod

        # 8개가 모두 생성되고 version 2인 경우만 내부 로직 진행
        query = f"select * from Dy_story_event_prod where user_id='{user_id}' and time_stamp='{time_stamp}';"
        result_mode_check = dynamodb_client.execute_statement(Statement=query)

        version = "1"
        try:
            if result_mode_check["Items"][0]["mode"]["S"]:
                version = "2"
        except:
            version = "1"

        # presigned url 발급!! (초기는 모든 페이지의 index가 1로 고정됨)
        dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")
        table = dynamodb.Table(f"Dy_story_image_prod")

        temp_dict = {}
        temp_dict["user_id"] = user_id
        temp_dict["time_stamp"] = time_stamp

        # 현재는 8페이지 고정
        temp_dict["page_index"] = "11111111"

        s3_client = boto3.client("s3")
        client_action = "get_object"
        bucket_pre = "s3-kkobook-story-image"

        for page_num in range(1, 9):
            time.sleep(0.5)
            # 처음엔 1페이지 고정!
            key = f"upscale/{user_id}/{time_stamp}/{page_num}/1.jpg"
            url = generate_presigned_url(
                s3_client,
                client_action,
                {"Bucket": bucket_pre, "Key": key},
                604800,
            )
            temp_dict[str(page_num)] = url

        temp = table.put_item(Item=temp_dict)

        # dynamodb processing (정리)
        query = f"UPDATE Dy_story_event_{env} SET status = 'finish' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
        result = dynamodb_client.execute_statement(Statement=query)

        # title 가져오기
        query = f"select title from Dy_story_event_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
        result = dynamodb_client.execute_statement(Statement=query)

        title = result["Items"][0]["title"]["S"]

        # Dy_user_book 업데이트!
        dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")

        # dynamodb update
        query = f"UPDATE Dy_user_book_{env} SET status = 'finish' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
        result = dynamodb_client.execute_statement(Statement=query)

        query = f"UPDATE Dy_user_book_{env} SET title = '{title}' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
        result = dynamodb_client.execute_statement(Statement=query)

        # firebase에 noti 보내기
        reqUrl = "firebase url"

        headersList = {"Content-Type": "application/json"}

        payload = json.dumps({"user_id": f"{user_id}"})

        response = requests.request("POST", reqUrl, data=payload, headers=headersList)

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
