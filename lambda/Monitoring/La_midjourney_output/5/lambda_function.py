import json
import boto3
import time
import requests
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from JsonMorph import JsonMorph


# upscale 된 후의 image와 되지 않은 상태의 image를 따로 구분할 필요가 있을

def lambda_handler(event, context):
    
    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]
    
    datetime_utc = datetime.utcnow()
    timezone_kst = timezone(timedelta(hours=9))
    # 현재 한국 시간
    datetime_kst = datetime_utc.astimezone(timezone_kst)
    
    # dynamodb
    dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
    table=dynamodb.Table(f"Dy_train_data")
    
    dynamodb_client=boto3.client('dynamodb')
    
    # 기존 DB 업데이트 한 후 결과 저장.
    
    # from ec2
    try:
        temp=str(datetime_kst.timestamp())
        temp=temp.split(".")
        temp=temp[0]+temp[1][:3]
        time_sort_key=temp

        # message_body: object_key, message_id, job_hash, img_url, state
        message_body=event        
        pk=message_body['object_key']
        message_body['pk']=pk
    except:
        print("why?")
    
    # Dy_midjourney_check_prod 상태 업데이트!!
    try:
        dynamodb_client = boto3.client('dynamodb', region_name='ap-northeast-2')
        query=f"UPDATE Dy_midjourney_check_dev SET \"check\" = 'end' WHERE pk='{pk}';"
        result=dynamodb_client.execute_statement(Statement=query)
    except:
        print("dynamodb update fail!")

    try:
        temp=table.put_item(
            Item=message_body
        )
    except:
        print("put error!")
        
    print("Good!")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
