import json
import boto3
import time
import traceback
import requests
import random
import openai
import os
import urllib.request
from io import BytesIO
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv("key.env")
openai.api_key = os.getenv("OPEN_API_KEY")


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


def make_stroy(
    messages, temperature=1, top_p=1, n=1, presence_penalty=0, frequency_penalty=0
):
    chat_response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages,
        temperature=temperature,
        top_p=top_p,
        n=n,
        presence_penalty=presence_penalty,
        frequency_penalty=frequency_penalty,
    )
    output = chat_response["choices"][0]["message"]["content"]
    return output


def make_stroy_time_out_50sec(
    messages, temperature=1, top_p=1, n=1, presence_penalty=0, frequency_penalty=0
):
    chat_response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=messages,
        temperature=temperature,
        top_p=top_p,
        n=n,
        presence_penalty=presence_penalty,
        frequency_penalty=frequency_penalty,
        request_timeout=80,
    )
    output = chat_response["choices"][0]["message"]["content"]
    return output


def lambda_handler(event, context):
    print(event)

    dynamodb_client = boto3.client("dynamodb")
    dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")

    issue_type = event["type"]

    if issue_type == "quiz":
        user_id = event["user_id"]
        time_stamp = event["time_stamp"]

        env = "prod"

        table_story_quiz = dynamodb.Table(f"Dy_story_quiz_prod")

        query = f"select * from Dy_user_book_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
        result = dynamodb_client.execute_statement(Statement=query)
        for item in result["Items"]:
            story = ""
            user_id = item["user_id"]["S"]
            time_stamp = item["time_stamp"]["S"]

            query = f"select * from Dy_gpt_story_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
            result_story = dynamodb_client.execute_statement(Statement=query)

            for i in range(1, 9):
                story += " " + result_story["Items"][0][str(i)]["S"]

            print(story)
            # title 가져오기
            query = f"select title from Dy_user_book_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
            result_title = dynamodb_client.execute_statement(Statement=query)
            title = result_title["Items"][0]["title"]["S"]
            # print(title)

            message = [
                {
                    "role": "user",
                    "content": "너는 주어진 동화에 대한 퀴즈를 만들어주는 작가 선생님으로, 아이들에게 이야기해주는 톤을 유지하며, 아래의 동화제목, 동화내용을 기반으로 제약사항을 참고하여 아래 출력형식(JSON)에 맞춰 퀴즈를 생성해라. \n 동화제목: %(title)s \n 동화내용: %(story)s \n 제약사항: \n 최대 question 개수: 4개; \n question 개수: 4개; \n choices수: 4개; \n 출력형식:{quiz_title:"
                    ",quiz_questions:{[question:"
                    ", choices:"
                    ",correct_answer:"
                    "},{question:"
                    ", choices:"
                    ",correct_answer:"
                    "},{question:"
                    ", choices:"
                    ",correct_answer:"
                    "},{question:"
                    ", choices:"
                    ",correct_answer:}]}"
                    % {
                        "title": title,
                        "story": story,
                    },
                }
            ]
            # print(message)
            quiz_temp = {}
            quiz_temp["user_id"] = user_id
            quiz_temp["time_stamp"] = time_stamp

            cnt = 0
            flag = False
            while cnt < 3:
                try:
                    story_quiz = make_stroy_time_out_50sec(message, temperature=1)
                    story_json = json.loads(story_quiz)
                    print(story_json["quiz_title"])
                    print(story_json["quiz_questions"])
                    number = 1
                    for question_temp in story_json["quiz_questions"]:
                        question = question_temp["question"]
                        choices = question_temp["choices"]
                        correct_answer = question_temp["correct_answer"]
                        quiz_temp[f"{number}_question"] = question
                        quiz_temp[f"{number}_choices"] = choices
                        quiz_temp[f"{number}_correct_answer"] = correct_answer
                        number += 1

                    flag = True
                    break
                except:
                    err_msg = traceback.format_exc()
                    # print("2", err_msg)
                    print("time_out 발생")
                    cnt += 1

                    # return {"statusCode": 200, "body": "why??"}
            # print(story_json)
            # print(quiz_temp)
            if flag:
                temp = table_story_quiz.put_item(Item=quiz_temp)
            else:
                post_slack(
                    f"api_issue_handling lambda error occur! issue_type:quiz {user_id}, {time_stamp}"
                )

        # quiz 복구 프로새스 자동화 진
    elif issue_type == "eng_story":
        print("hi")
        user_id = event["user_id"]
        time_stamp = event["time_stamp"]
        # post_slack(f"english story request issue occur! issue_type:eng_story {user_id}, {time_stamp}")

        # 영어버전 생성 << text에 대해서만 진행
        try:
            print(user_id, time_stamp)
            query = f"select * from Dy_gpt_story_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
            story_result = dynamodb_client.execute_statement(Statement=query)

            # print(story_result)

            query_title = f"select * from Dy_story_event_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
            story_title = dynamodb_client.execute_statement(Statement=query_title)

            # print(story_title)

            title = story_title["Items"][0]["title"]["S"]

            story = ""
            story += f"title:{title}\n"

            for i in range(1, 9):
                temp = story_result["Items"][0][str(i)]["S"]
                story += f"{i}:{temp}\n"

            print(story)
            message = [
                {
                    "role": "user",
                    "content": "너는 아이들을 위한 동화를 번역하는 번역가로 한국어로 작성된 다음의 동화 내용을 이해하고 해당 내용을 영어로 제약조건과 출력형식(JSON)을 반드시 지켜 번역해줘.\n동화내용:\n%(story)s\n제약조건:\n3세 유아가 이해할 수 있는 어휘를 사용한다.\n새로운 내용을 추가하지 않는다.\n기존의 내용을 그대로 영어로 번역한다.\n\n출력형식:\n{title:동화의 제목, 1:페이지 내용, 2:페이지 내용, 3:페이지 내용, 4:페이지 내용, 5:페이지 내용, 6:페이지 내용, 7:페이지 내용, 8:페이지 내용}"
                    % {
                        "story": story,
                    },
                }
            ]

            cnt = 0
            while cnt < 3:
                try:
                    story_eng = make_stroy_time_out_50sec(message, temperature=1)
                    story_json = json.loads(story_eng)
                    print(story_json)
                    break
                except:
                    err_msg = traceback.format_exc()
                    print("time_out 발생!")
                    # print("2", err_msg)
                    # return {"statusCode": 200, "body": "why??"}
                    cnt += 1

            if cnt == 3:
                print("english put fail!")
                notification = f"from La_post_gpt: user_id:{user_id}, time_stamp:{time_stamp}, reason: english version fail!!"

            # DB put 작업진행
            try:
                story_json["user_id"] = user_id
                story_json["time_stamp"] = time_stamp
                table = dynamodb.Table(f"Dy_gpt_story_english_prod")
                temp = table.put_item(Item=story_json)
            except:
                print("english put fail!")
                # notification = f"from La_post_gpt: user_id:{user_id}, time_stamp:{time_stamp}, reason: english version fail!!"
                # post_slack(notification)
                print(story_eng)

        except:
            # notification = f"from La_post_gpt: user_id:{user_id}, time_stamp:{time_stamp}, reason: english version fail!!"
            # post_slack(notification)
            print("english version fail!")

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
