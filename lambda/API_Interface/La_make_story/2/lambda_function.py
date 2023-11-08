import json
import boto3
import time
import random
from datetime import datetime, timedelta, timezone
# 유저에게 post로 대분류, 중분류, 소분류, 캐릭터_datetime을 전달 받음

def lambda_handler(event, context):
    
    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]

    dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
    table=dynamodb.Table(f"Dy_story_event_{env}")
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
        
        user_id=item['user_id']
        # 동화 생성은 한번에 하나씩만
        query=f"select * from Dy_story_event_{env} where user_id='{item['user_id']}' and status='ongoing';"
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
        query=f"select age,gender,name from Dy_user_character_{env} where user_id='{item['user_id']}' and time_stamp='{item['character_datetime']}';"
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
    
    # put Dy_user_book (클라이언트가 책이 만들어지고 있음을 알기 위해)
    try:
        # Dy_user_book 추가
        dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
        table=dynamodb.Table(f"Dy_user_book_{env}")
        item={}
        # 존재한다면 S3에 책이 만들어진 상태! 덮어쓰기!
        item['user_id']=user_id
        item['time_stamp']=str(int(datetime_kst.timestamp()))
        item["title"]=""
        item['status']='ongoing'
        item["num"]=str(random.randint(0,5))
        
        temp=table.put_item(
            Item=item
        )
        
    except:
        print("put dynamodb fail")
        statusCode=500
        return {
            'statusCode': statusCode,
            'body': json.dumps('Internet Server Error')
        }


    # sqs 전송 (SQS_make_story)
    try:
        # 최종 처리는 sqs에 연결된 lambda가 진행
        sqs = boto3.resource('sqs', region_name='ap-northeast-2')
        queue = sqs.get_queue_by_name(QueueName=f"SQS_make_story_{env}")
        
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
