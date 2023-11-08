import json
import boto3
import os
import requests
import time
from dotenv import load_dotenv
from JsonMorph import JsonMorph

# La_upscale_character



def lambda_handler(event, context):
    
    dynamodb_client=boto3.client('dynamodb')
    # print(event)
    # 상태코드
    statusCode=200

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
        query=f"select datetime from Dy_character_event where user_id='{user_id}' and status='ongoing'"
        result=dynamodb_client.execute_statement(Statement=query)
        #print(result)
        
        if len(result["Items"])!=1:
            statusCode=400
            return {
                'statusCode': statusCode,
                'body': json.dumps('Bad Request')
            }
            
        datetime=result["Items"][0]['datetime']['S']
        # index 정보
        item=event['body']
        item=json.loads(item)
        
        # upscale index
        index=int(item['index'])
        # 최종 user name
        name=str(item['name'])

    except:
        statusCode=400
        return {
            'statusCode': statusCode,
            'body': json.dumps('Bad Request')
        }
    '''
    # get image_information
    try:
        query=f"select img_url,job_hash,message_id from Dy_midjourney_output_character where user_id='{user_id}' and state='before' and datetime='{datetime}'"
        result=dynamodb_client.execute_statement(Statement=query)
        print(result)
        if len(result["Items"])==0:
            statusCode=503
            return {
                'statusCode': statusCode,
                'body': json.dumps('Service Unavailable')
            }
            
        img_url=result["Items"][0]['img_url']['S']
        job_hash=result["Items"][0]['job_hash']['S']
        message_id=result["Items"][0]['message_id']['S']
    except:
        statusCode=503
        return {
            'statusCode': statusCode,
            'body': json.dumps('Service Unavailable')
        }
    
    # post upscale request
    try:
        load_dotenv("key.env")
        MID_JOURNEY_ID = os.getenv("MID_JOURNEY_ID")
        SERVER_ID = os.getenv("SERVER_ID")
        CHANNEL_ID = os.getenv("CHANNEL_ID")
        header = {'authorization' : os.getenv("VIP_TOKEN")}
        URL = "https://discord.com/api/v9/interactions"
        StorageURL = "https://discord.com/api/v9/channels/" + CHANNEL_ID + "/attachments"
        
        # just test
        # index=4
        __payload = JsonMorph(MID_JOURNEY_ID, SERVER_ID, CHANNEL_ID, index, message_id, job_hash, "upsample")
        response =  requests.post(url = URL, json = __payload, headers = header)

    except:
        statusCode=500
        return {
            'statusCode': statusCode,
            'body': json.dumps('Internal Server error')
        }
    '''
    # get upscale image!
    while True:
        time.sleep(1)
        index=str(index)
        query=f"select img_url from Dy_midjourney_output_character_upscale where user_id='{user_id}' and datetime='{datetime}' and in_dex='{index}'"
        print(query)
        result=dynamodb_client.execute_statement(Statement=query)
        #print(result)
        if len(result["Items"])!=0:
            img_url=result["Items"][0]['img_url']['S']
            try:
                query=f"UPDATE Dy_character_event SET status = 'finish' WHERE user_id='{user_id}' and datetime='{datetime}';"
                result=dynamodb_client.execute_statement(Statement=query)
                
            except:
                statusCode=500
                return {
                    'statusCode': statusCode,
                    'body': json.dumps('Internal Server error')
                }  
            try:
                # 최종 처리는 sqs에 연결된 lambda가 진행
                sqs = boto3.resource('sqs', region_name='ap-northeast-2')
                queue = sqs.get_queue_by_name(QueueName='SQS_character_finish_processing')
                temp_json={}
                temp_json['user_id']=user_id
                temp_json['datetime']=datetime
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
            break
    
    # return upscale image url
    bodyData={"img_url":img_url}
    jsonData = json.dumps(bodyData, ensure_ascii=False).encode('utf8')
    
    print("Good!")
    return {
        'statusCode': statusCode,
        'body': jsonData
    }