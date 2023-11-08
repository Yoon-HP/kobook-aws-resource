import json
import boto3
import time
from datetime import datetime, timedelta, timezone
# 유저에게 post로 대분류, 중분류, 소분류, 캐릭터_datetime을 전달 받음

def lambda_handler(event, context):
    
    dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
    table=dynamodb.Table("Dy_story_event")
    dynamodb_client=boto3.client('dynamodb')
    
    # 시간
    datetime_utc = datetime.utcnow()
    timezone_kst = timezone(timedelta(hours=9))
    # 현재 한국 시간
    datetime_kst = datetime_utc.astimezone(timezone_kst)  
    
    statusCode=200
    try:
        httpMethod=event['httpMethod']
    except:
        print("firebase user!!")
        
    try:
        httpMethod=event['routeKey'].split()[0]
    except:
        print("cognito user!!")

    if httpMethod!="POST":
        statusCode=405
        return {
            'statusCode': statusCode,
            'body': json.dumps('Method Not Allowed')
        }
    # 이전 상태 확인
    try:
        # major, middle, sub, character_datetime
        item=event['body']
        item=json.loads(item)
        
        # cognito
        try:
            item['user_id']=event['requestContext']['authorizer']['claims']['cognito:username']
        except:
            print("firebase user!!")
        
        # firebase
        try:
            item['user_id']=event['requestContext']['authorizer']['jwt']['claims']['user_id']
        except:
            print("cognito user!!")
        
        
        # 동화 생성은 한번에 하나씩만
        query=f"select * from Dy_story_event where user_id='{item['user_id']}' and status='ongoing';"
        result=dynamodb_client.execute_statement(Statement=query)
        if len(result['Items']):
            print("story fail!!")
            return {
                'statusCode': 400,
                'body': json.dumps("Bad Request")
            }
        
    except:
        print("What error?")
        statusCode=500
        return {
            'statusCode': statusCode,
            'body': json.dumps('Internal Server Error!')
        }
    
    # put dynamodb!!
    try:
        # name은 공란 (향후 채워넣어야 함)
        #item['datetime']=datetime_kst.strftime('%Y-%m-%d-%H-%M-%S')
        item['time_stamp']=str(int(datetime_kst.timestamp()))
        item['status']='ongoing'
        # item['fail']="no"
        print(item)
        # get character_information!!!
        query=f"select age,gender,name from Dy_user_character where user_id='{item['user_id']}' and datetime='{item['character_datetime']}';"
        result=dynamodb_client.execute_statement(Statement=query)
        age=result['Items'][0]['age']['N']
        gender=result['Items'][0]['gender']['S']
        name=result['Items'][0]['name']['S']
        
        item['age']=age
        item['gender']=gender
        item['name']=name
        item['title']="def"
        
        temp=table.put_item(
            Item=item
        )

    except:
        print("put dynamodb fail")
        statusCode=400
        return {
            'statusCode': statusCode,
            'body': json.dumps('Bad Request')
        }

    # sqs 전송 (SQS_make_story)
    try:
        # 최종 처리는 sqs에 연결된 lambda가 진행
        sqs = boto3.resource('sqs', region_name='ap-northeast-2')
        queue = sqs.get_queue_by_name(QueueName='SQS_make_story')
        
        user_id=item['user_id']
        time_stamp=item['time_stamp']
        
        temp_json={}
        temp_json['user_id']=user_id
        temp_json['time_stamp']=time_stamp
        message_body=json.dumps(temp_json)
        response = queue.send_message(
            MessageBody=message_body,
        )
    except ClientError as error:
        logger.exception("Send Upscale message failed: %s", message_body)
        raise error
        
    print("good")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
