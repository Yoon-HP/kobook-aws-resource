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

    
    # post midjourney!!!
    try:
        load_dotenv("key.env")
        MID_JOURNEY_ID = os.getenv("MID_JOURNEY_ID")
        SERVER_ID = os.getenv("SERVER_ID")
        CHANNEL_ID = os.getenv("CHANNEL_ID")
        header = {'authorization' : os.getenv("VIP_TOKEN")}
        URL = "https://discord.com/api/v9/interactions"
        StorageURL = "https://discord.com/api/v9/channels/" + CHANNEL_ID + "/attachments"


        # 페이지 수 만큼 요청을 보내야 함!
        
        for page_index in range(len(keys)):
            
            gpt_prompt=result["Items"][0][keys[page_index]]['S']
            
            try:
                # Dy_midjourney_check_prod 기록
                datetime_utc = datetime.utcnow()
                timezone_kst = timezone(timedelta(hours=9))
                # 현재 한국 시간
                datetime_kst = datetime_utc.astimezone(timezone_kst)
                temp=str(datetime_kst.timestamp())
                temp=temp.split(".")
                temp=temp[0]+temp[1][:3]
                time_sort_key=temp
                
                # dynamodb !! character와 story 분리 예정
                dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
                table=dynamodb.Table(f"Dy_midjourney_check_{env}")
                message_body={}
                message_body['pk']=f"s_t/{user_id}/{time_stamp}/{page_index+1}"
                message_body['time']=time_sort_key
                message_body['check']="no"
            except:
                print("put_dynamodb fail!!")

            # 주인공 등장
            if page_index%2==0:
                # style 통일!!! (고민) turbo 사용 X
                prompt=f"<#s_t/{user_id}/{time_stamp}/{page_index+1}> {img_url} {gpt_prompt}, {style}"
                print(prompt)
                
                
                message_body['prompt']=prompt
                
                '''
                try:
                    # dynamodb put!
                    temp=table.put_item(
                        Item=message_body
                    )
                except:
                    print("dynamodb put fail!!")
                '''
                
                __payload=JsonImagine(MID_JOURNEY_ID, SERVER_ID, CHANNEL_ID, prompt)
                # post to midjourney!!!
                while True:
                    response = requests.post(url = URL, json = __payload, headers = header)
                    if response.status_code!=204:
                        time.sleep(1)
                    else:
                        break
                
                time.sleep(40)
            
            # 배경만 등장 <- 이 방향으로 가지 않을 것...
            else:
                # style 통일!!! (고민) turbo 사용 X
                # prompt=f"<#s_t/{user_id}/{time_stamp}/{page_index+1}> {gpt_prompt}, {style}, only background"
                prompt=f"<#s_t/{user_id}/{time_stamp}/{page_index+1}> {img_url} {gpt_prompt}, {style}"
                print(prompt)
                
                
                message_body['prompt']=prompt
                '''
                try:
                    # dynamodb put!
                    temp=table.put_item(
                        Item=message_body
                    )
                except:
                    print("dynamodb put fail!!")
                '''
                    
                __payload=JsonImagine(MID_JOURNEY_ID, SERVER_ID, CHANNEL_ID, prompt)
                # post to midjourney!!!
                while True:
                    response = requests.post(url = URL, json = __payload, headers = header)
                    if response.status_code!=204:
                        time.sleep(1)
                    else:
                        break
                        
                time.sleep(40)
            
    except:
        print("OTL...")
        
    print("Good!")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello La_post_midjourney_story')
    }
