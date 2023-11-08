import json
import boto3
import time
import os
import requests
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from JsonImagine import JsonImagine

# to filo.team.dev2@gmail.com discord channel

# dynamodb
dynamodb_client=boto3.client('dynamodb')
def lambda_handler(event, context):
    
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]
    
    # event parsing (from sqs) user_id, time_stamp 전달 받음
    # print(event)
    '''
    try:
        # print(event)
        message_body=json.loads(event["Records"][0]['body'])
        # bucket_name=message_body['Records'][0]['s3']['bucket']['name']
        # object_key=message_body['Records'][0]['s3']['object']['key']
        
        
        user_id=message_body['user_id']
        time_stamp=message_body["time_stamp"]

        query=f"SELECT character_datetime FROM Dy_story_event_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
        print(query)
        result=dynamodb_client.execute_statement(Statement=query)
        print(result)
        print("check!!")
        
        character_datetime=result['Items'][0]['character_datetime']['S']
        print(character_datetime)
        
        
        # character datetime 먼저 구한 후 이를 바탕으로 이미지 url 가져오기
        query=f"SELECT img_url,style FROM Dy_user_character_{env} where user_id='{user_id}' and time_stamp='{character_datetime}'"
        result=dynamodb_client.execute_statement(Statement=query)
        print(result)
        print("check!!")
        img_url=result['Items'][0]['img_url']['S']
        style=result['Items'][0]['style']['S']
        
        # gpt prompt 가져오기 (from Dy_gpt_prompt)
        
        query=f"SELECT * FROM Dy_gpt_prompt_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
        result=dynamodb_client.execute_statement(Statement=query)
        print(result)
        print("check!!")
        
        try:
            temp=list(result["Items"][0].keys())
            temp.remove('user_id')
            temp.remove('time_stamp')
            temp.sort()
            keys=temp
        except:
            print("parsing fail!")
            return {
                'statusCode': 200,
                'body': json.dumps('Hello')
            }    
        
    except:
        print("event_parsing_fail!!!!!")
        return {
            'statusCode': 200,
            'body': json.dumps('Hello La_post_midjourney_character')
        }
    '''
    # lambda invoke 
    try:
        object_key=event['pk']
        mode=event['mode']
        print(mode)
        print(object_key)
        user_id=object_key.split('/')[1]
        time_stamp=object_key.split('/')[2]
        print(user_id,time_stamp)
        #print("check!!")
        # query와 관련한 이슈가 발생하는 경우가 존재함.
        ct=0
        while ct<5:
            try:
                dynamodb_client=boto3.client('dynamodb')
                query=f"SELECT prompt FROM Dy_midjourney_check_story_{env} where pk='{object_key}' and mode='{mode}'"
                print(query)
                result=dynamodb_client.execute_statement(Statement=query)
                print(result)
                prompt=result["Items"][0]['prompt']['S']
                break
            except:
                print("why?")
                time.sleep(1)
                ct+=1
    except:
        print("hello sqs")

    
    # post midjourney!!!
    try:
        load_dotenv("key.env")
        MID_JOURNEY_ID = os.getenv("MID_JOURNEY_ID")
        SERVER_ID = os.getenv("SERVER_ID")
        CHANNEL_ID = os.getenv("CHANNEL_ID")
        header = {'authorization' : os.getenv("VIP_TOKEN")}
        URL = "https://discord.com/api/v9/interactions"
        StorageURL = "https://discord.com/api/v9/channels/" + CHANNEL_ID + "/attachments"

        # Dy_midjourney_check_prod 기록
        datetime_utc = datetime.utcnow()
        timezone_kst = timezone(timedelta(hours=9))
        # 현재 한국 시간
        datetime_kst = datetime_utc.astimezone(timezone_kst)
        temp=str(datetime_kst.timestamp())
        temp=temp.split(".")
        temp=temp[0]+temp[1][:3]
        time_sort_key=temp
        
        try:
            dynamodb_client=boto3.client('dynamodb')
            query=f"UPDATE Dy_midjourney_check_story_{env} SET \"time\" = '{time_sort_key}' WHERE pk='{object_key}' and mode='{mode}';"
            result=dynamodb_client.execute_statement(Statement=query)
        except:
            print("dynamodb update fail!")

        __payload=JsonImagine(MID_JOURNEY_ID, SERVER_ID, CHANNEL_ID, prompt)
        # post to midjourney!!!
        while True:
            response = requests.post(url = URL, json = __payload, headers = header)
            if response.status_code!=204:
                time.sleep(1)
            else:
                break
    except:
        print("OTL...")
        
    # appeal monitoring lambda invoke!
    try:
        lambda_client=boto3.client('lambda')
        
        payload={
            'pk':object_key,
            'time':time_sort_key,
            'prompt':prompt,
            'mode':mode
        }
        response = lambda_client.invoke(
            FunctionName='La_midjourney_check_story:prod',
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
    except:
        print("lambda invoke fail!")
        
    print("Good!")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello La_post_midjourney_story')
    }
