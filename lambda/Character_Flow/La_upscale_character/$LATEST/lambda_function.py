import json
import boto3
import os
import requests
import time
from dotenv import load_dotenv
from JsonMorph import JsonMorph
from presigned_url import generate_presigned_url

# La_upscale_character

def lambda_handler(event, context):
    
    dynamodb_client=boto3.client('dynamodb')
    # print(event)
    # 상태코드
    statusCode=200

    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]
    
    # ping check
    try:
        if 'ping' in event:
            print("ping!!")
            return {
                'statusCode': 200,
                'body': json.dumps('Hello from Lambda!')
            }
    except:
        print("keep going")


    # event parsing, get unique datetime
    try:
        # cognito
        try:
            user_id=event['requestContext']['authorizer']['claims']['cognito:username']
            httpMethod=event['httpMethod']
        except:
            print("firebase user!!")
        
        # firebase
        try:
            user_id=event['requestContext']['authorizer']['jwt']['claims']['user_id']
            httpMethod=event['routeKey'].split()[0]
        except:
            print("cognito user!!")
        
        
        if httpMethod!="POST":
            statusCode=405
            return {
                'statusCode': statusCode,
                'body': json.dumps('Method Not Allowed')
            }
    

        # get datetime <- ongoing 중인 작업은 유저당 하나여야 함!!!
        query=f"select time_stamp from Dy_character_event_{env} where user_id='{user_id}' and status='ongoing'"
        result=dynamodb_client.execute_statement(Statement=query)
        #print(result)
        
        if len(result["Items"])!=1:
            statusCode=400
            return {
                'statusCode': statusCode,
                'body': json.dumps('Bad Request')
            }
            
        time_stamp=result["Items"][0]['time_stamp']['S']
        # index 정보
        item=event['body']
        item=json.loads(item)
        
        # upscale index
        index=str(item['index'])
        # 최종 user name
        name=str(item['name'])
        
        # time_stamp, user_id로 object key를 만든 후 s3에서 presigned url 만들기

    except:
        statusCode=400
        return {
            'statusCode': statusCode,
            'body': json.dumps('Bad Request')
        }

    # presigned url 발급
    try:
        s3_client=boto3.client('s3')
        bucket="s3-kkobook-character"
        key=f"upscale/{user_id}/{time_stamp}/{index}.jpg"
        print(key)
        client_action="get_object"
        img_url = generate_presigned_url(
            s3_client, client_action, {'Bucket': bucket, 'Key': key}, 604800)
    except:
        print("presigned url issue!")
        
        
    try:
        query=f"UPDATE Dy_character_event_{env} SET status = 'finish' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
        result=dynamodb_client.execute_statement(Statement=query)
        
        # dynamodb에 저장 Dy_user_character_{env}
        dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
        table=dynamodb.Table(f"Dy_user_character_{env}")
        temp_json={}
        temp_json['user_id']=user_id
        temp_json['time_stamp']=time_stamp
        
        # 해당 img_url을 어떻게 처리할지 고민 필요
        temp_json['img_url']=img_url
        temp_json['name']=name
        temp_json['object_key']=f"upscale/{user_id}/{time_stamp}/{index}.jpg"
        
        query=f"select age, gender, style, cloth from Dy_character_event_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
        result=dynamodb_client.execute_statement(Statement=query)
        
        temp_json['age']=int(result["Items"][0]['age']['N'])
        temp_json['gender']=str(result["Items"][0]['gender']['S'])
        temp_json['style']=str(result["Items"][0]['style']['S'])
        temp_json['cloth']=str(result["Items"][0]['cloth']['S'])
        
        temp=table.put_item(
            Item=temp_json
        )
                
    except:
        print("fail!!!")
        statusCode=500
        return {
            'statusCode': statusCode,
            'body': json.dumps('Internal Server error')
        }
    
    # 해당 기능 현재 사용 X
    '''
    try:
        # 최종 처리는 sqs에 연결된 lambda가 진행
        sqs = boto3.resource('sqs', region_name='ap-northeast-2')
        queue = sqs.get_queue_by_name(QueueName=f"SQS_character_finish_processing_{env}")
        temp_json={}
        temp_json['user_id']=user_id
        temp_json['time_stamp']=time_stamp
        temp_json['img_url']=img_url
        # 최종 이름도 저장
        temp_json['name']=name
        message_body=json.dumps(temp_json)
        response = queue.send_message(
            MessageBody=message_body,
        )
    except ClientError as error:
        logger.exception("Send Upscale message failed: %s", message_body)
        raise error
    '''
    # return upscale image url
    
    bodyData={"img_url":img_url}
    jsonData = json.dumps(bodyData, ensure_ascii=False).encode('utf8')
    
    print("Good!")
    return {
        'statusCode': statusCode,
        'body': jsonData
    }