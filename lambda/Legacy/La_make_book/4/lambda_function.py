import json
import boto3
import time
import traceback
import textwrap
import requests
import os
import random
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from write_to_image import write_to_image

# dynamodb
dynamodb_client = boto3.client("dynamodb")


def lambda_handler(event, context):
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env = function_arn.split(":")[-1]
    # event parsing (from sqs)
    try:
        print(event)
        message_body = json.loads(event["Records"][0]["body"])

        user_id = message_body["user_id"]
        time_stamp = message_body["time_stamp"]

        # get story!!
        query = f"SELECT * FROM Dy_gpt_story_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
        print(query)
        result_story = dynamodb_client.execute_statement(Statement=query)
        print(result_story)
        print("check!!")

        try:
            temp = list(result_story["Items"][0].keys())
            temp.remove("user_id")
            temp.remove("time_stamp")
            temp.sort()
            story_keys = temp
            print(story_keys)
        except:
            print("parsing fail!")
            return {"statusCode": 200, "body": json.dumps("Hello")}
        # get image!!
        query = f"SELECT * FROM Dy_midjourney_output_story_upscale_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
        print(query)
        result_image = dynamodb_client.execute_statement(Statement=query)
        print(result_image)
        print("check!!")

        # in_dex 별로 파싱 작업 필요
        image_in_dex = {}
        for item in result_image["Items"]:
            image_in_dex[item["in_dex"]["S"]] = item["img_url"]["S"]

        print(image_in_dex)

        try:
            temp = list(image_in_dex.keys())
            temp.sort()
            image_keys = temp
            print(image_keys)
        except:
            print("parsing fail!")
            return {"statusCode": 200, "body": json.dumps("Hello")}
    except:
        print("preprocessing fail!!")
        return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}

    # make book!! (1024,1024) 기준
    try:
        s3 = boto3.client("s3")
        # font download 향후 폰트 선택도 가능하게 확장가능
        s3.download_file(
            "s3-kkobook-processing",
            "font/TmoneyRoundWindExtraBold.ttf",
            "/tmp/font.tff",
        )

        # background 가져오기
        s3.download_file(
            "s3-kkobook-processing",
            "story_background/blue_background.jpg",
            "/tmp/background.jpg",
        )

        # 책 만들기
        for page_num in range(len(story_keys)):
            # page_num 짝수: 오른쪽 그림(등장인물) 왼쪽 글 홀수: 왼쪽 그림(배경) 오른쪽 글

            # 글 부분
            width = 20
            font_color = "rgb(0, 0, 0)"
            text = result_story["Items"][0][story_keys[page_num]]["S"]
            font_path = "/tmp/font.tff"
            result_path = "/tmp/story.png"
            background_path = "/tmp/background.jpg"

            write_to_image(
                text, width, font_path, font_color, result_path, background_path
            )

            # 이미지 부분
            img_url = image_in_dex[image_keys[page_num]]
            res = requests.get(img_url)
            img = Image.open(BytesIO(res.content))
            img = img.resize((1024, 1024))

            # thumbnail
            if page_num == 0:
                temp = img.resize((512, 512))
                temp.save("/tmp/thumbnail.png")
                s3.upload_file(
                    "/tmp/thumbnail.png",
                    "s3-kkobook-book",
                    f"{user_id}/{time_stamp}/thumbnail.png",
                )

            if page_num % 2 == 0:
                # merge
                story = Image.open("/tmp/story.png")
                new_image = Image.new(
                    "RGB", (2 * img.size[0], img.size[1]), (255, 255, 255)
                )
                new_image.paste(img, (0, 0))
                new_image.paste(story, (img.size[0], 0))
                new_image = new_image.resize((1024, 512))
                new_image.save("/tmp/page.png")
            else:
                # merge
                story = Image.open("/tmp/story.png")
                new_image = Image.new(
                    "RGB", (2 * img.size[0], img.size[1]), (255, 255, 255)
                )
                new_image.paste(story, (0, 0))
                new_image.paste(img, (story.size[0], 0))
                new_image = new_image.resize((1024, 512))
                new_image.save("/tmp/page.png")

            # s3 upload!
            s3.upload_file(
                "/tmp/page.png",
                "s3-kkobook-book",
                f"{user_id}/{time_stamp}/{page_num+1}.png",
            )

    except:
        print("make book error!!")
        err_msg = traceback.format_exc()
        print(err_msg)
        return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}

    try:
        # dynamodb processing (정리)
        query = f"UPDATE Dy_story_event_{env} SET status = 'finish' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
        result = dynamodb_client.execute_statement(Statement=query)

        # title 가져오기
        query = f"select title from Dy_story_event_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
        result = dynamodb_client.execute_statement(Statement=query)

        title = result["Items"][0]["title"]["S"]

        # Dy_user_book 업데이트!
        dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")
        table = dynamodb.Table(f"Dy_user_book_{env}")

        # dynamodb update
        query = f"UPDATE Dy_user_book_{env} SET status = 'finish' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
        result = dynamodb_client.execute_statement(Statement=query)

        query = f"UPDATE Dy_user_book_{env} SET title = '{title}' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
        result = dynamodb_client.execute_statement(Statement=query)

        """
        item={}
        # 존재한다면 S3에 책이 만들어진 상태! 덮어쓰기!
        item['user_id']=user_id
        item['time_stamp']=time_stamp
        item["title"]=title
        item['status']='finish'
        # item["num"]=str(random.randint(0,5))

        
        # put dynamodb!
        temp=table.put_item(
            Item=item
        )
        """

    except:
        print("dynamodb process fail!")
        err_msg = traceback.format_exc()
        print(err_msg)
        return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}

    try:
        # firebase에 noti 보내기
        reqUrl = "firebase url"

        headersList = {"Content-Type": "application/json"}

        payload = json.dumps({"user_id": f"{user_id}"})

        response = requests.request("POST", reqUrl, data=payload, headers=headersList)
    except:
        print("firebase noti fail!!")

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
