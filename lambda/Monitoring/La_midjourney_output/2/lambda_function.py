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
    table_before=dynamodb.Table(f"Dy_midjourney_output_character_{env}")
    table_after=dynamodb.Table(f"Dy_midjourney_output_character_upscale_{env}")

    table_before_story=dynamodb.Table(f"Dy_midjourney_output_story_{env}")
    table_after_story=dynamodb.Table(f"Dy_midjourney_output_story_upscale_{env}")
    
    dynamodb_client=boto3.client('dynamodb')
        
    # TODO implement
    # print(event)
    # event parsing (from sqs) s3->sqs flow와 event flow 구성이 다름!
    try:
        # message_body: object_key, message_id, job_hash, img_url, state
        message_body=json.loads(event["Records"][0]['body'])
        temp=str(datetime_kst.timestamp())
        temp=temp.split(".")
        temp=temp[0]+temp[1][:3]
        time_sort_key=temp
        # print(message_body)
        # dynamodb에 query 가능
        
        
        pk=message_body['object_key']
        # character
        if "s_t" not in message_body['object_key']:
            user_id=message_body['object_key'].split('/')[1]
            time_stamp=message_body['object_key'].split('/')[2].split('.')[0]
        else:
            # story!!!
            user_id=message_body['object_key'].split('/')[1]
            time_stamp=message_body['object_key'].split('/')[2]
            in_dex=message_body['object_key'].split('/')[3]
            
    except:
        print("event_parsing_fail!!!!!")
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }

    # put dynamodb
    try:

        # character!!!
        if "s_t" not in message_body['object_key']:
            message_body['user_id']=user_id
            message_body['time_stamp']=time_stamp
            
            if message_body['state']=='before':
                message_body['time']=time_sort_key
                
                print(message_body)
                temp=table_before.put_item(
                    Item=message_body
                )
                img_url=message_body['img_url']
                job_hash=message_body['job_hash']
                message_id=message_body['message_id']
                
                # Dy_midjourney_check_prod 상태 업데이트!!
                try:
                    dynamodb_client = boto3.client('dynamodb', region_name='ap-northeast-2')
                    query=f"UPDATE Dy_midjourney_check_prod SET \"check\" = 'end' WHERE pk='{pk}';"
                    result=dynamodb_client.execute_statement(Statement=query)
                except:
                    print("dynamodb update fail!")


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
                    for index in range(1,5):
                        __payload = JsonMorph(MID_JOURNEY_ID, SERVER_ID, CHANNEL_ID, index, message_id, job_hash, "upsample")
                        response =  requests.post(url = URL, json = __payload, headers = header)
                        time.sleep(1)
                except:
                    print("Not!!")
                    statusCode=500
                    return {
                        'statusCode': statusCode,
                        'body': json.dumps('!!!!!')
                    }
                    

                # 현재 디스코드 채널 상의 job 개수를 고려해 sqs에 전달 (1개)
                try:
                    query=f"SELECT * from Dy_midjourney_check_{env} where \"check\"='start'"
                    result=dynamodb_client.execute_statement(Statement=query)
                    print(result)
                    
                    # 시간순으로 오래된 사람부터 처리
                    time_sort=[]
                    for item in result["Items"]:
                        pk=item['pk']['S']
                        time_stamp=pk.split('/')[2].split('.')[0]
                        time_sort.append([time_stamp,pk])
                    
                    time_sort.sort()        
                    print(time_sort)
                    
                    # job 2개는 남겨놓을 예정..
                    if (10-job_number)>0 and time_sort:
                        try:
                            # 최종 처리는 sqs에 연결된 lambda가 진행
                            sqs = boto3.resource('sqs', region_name='ap-northeast-2')
                            queue = sqs.get_queue_by_name(QueueName=f"SQS_post_midjourney_character_{env}")
                            
                            # key에 pk 넣기
                            key=time_sort[0][1]
                            print(key)
                            temp_json={}
                            temp_json['key']=key
                            
                            # sqs의 시간지연으로 인해 발생할 수 있는 문제를 방지하기 위해 dynamodb update
                            try:
                                # dynamodb
                                dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
                                table=dynamodb.Table(f"Dy_midjourney_check_{env}")
                                message_body_dy={}
                                message_body_dy['pk']=key
                                message_body_dy['check']="no"
                                # dynamodb put!
                                temp=table.put_item(
                                    Item=message_body_dy
                                )
                            except:
                                print("dynamodb put fail!!")
                            
                            message_body_sqs=json.dumps(temp_json)
                            response = queue.send_message(
                                MessageBody=message_body_sqs,
                            )
                        except:
                            print("something went wrong!!!!")
                            # slack noti 추가!
                except:
                    print("something went wrong!!")
                    
                
            # 향후 index별로 관리할 수 있도록 time col을 정렬키로 사용함
            elif message_body['state']=='after':
                print("after:",message_body)
                message_body['time']=time_sort_key
                temp=table_after.put_item(
                    Item=message_body
                )
        else:
            
            # 유저가 동화 생성 과정 중 회원탈퇴를 진행하는 경우 로직이 꼬이는 문제가 발생할 수 있으므로 DB에 해당 user_id가 존재하는지 확인 후 진행
            # story!!!
            message_body['user_id']=user_id
            message_body['time_stamp']=time_stamp
            message_body['in_dex']=in_dex

            
            # db check 작업 > 해당 유저가 story_event에 존재하지 않으면 삭제된 것!
            dynamodb_client=boto3.client('dynamodb')
            # ongoing 상태까지 확인함으로 내부적으로 로직 테스트를 하더라도 문제가 발생하지 않음! (이전의 데이터를 가지고 하기에)
            query=f"select * from Dy_story_event_{env} where user_id='{user_id}' and status='ongoing';"
            result=dynamodb_client.execute_statement(Statement=query)
            print(result)
            if len(result['Items'])==0:
                print("회원 탈퇴한 유저!!")
                return {
                    'statusCode': 200,
                    'body': json.dumps("bye!")
                }
            
            if message_body['state']=='before':
                message_body['time']=time_sort_key
                
                print(message_body)
                temp=table_before_story.put_item(
                    Item=message_body
                )
                img_url=message_body['img_url']
                job_hash=message_body['job_hash']
                message_id=message_body['message_id']
                
                print("hello")
                # post upscale request
                '''
                try:
                    load_dotenv("key.env")
                    MID_JOURNEY_ID = os.getenv("MID_JOURNEY_ID")
                    SERVER_ID = os.getenv("SERVER_ID")
                    CHANNEL_ID = os.getenv("CHANNEL_ID")
                    header = {'authorization' : os.getenv("VIP_TOKEN")}
                    URL = "https://discord.com/api/v9/interactions"
                    StorageURL = "https://discord.com/api/v9/channels/" + CHANNEL_ID + "/attachments"
                    
                    # 일단 index 1번 고정!!
                    for index in range(1,2):
                        __payload = JsonMorph(MID_JOURNEY_ID, SERVER_ID, CHANNEL_ID, index, message_id, job_hash, "upsample")
                        response =  requests.post(url = URL, json = __payload, headers = header)
                        time.sleep(1)
                except:
                    print("Not!!")
                    statusCode=500
                    return {
                        'statusCode': statusCode,
                        'body': json.dumps('!!!!!')
                    }
                '''
            # 향후 index별로 관리할 수 있도록 time col을 정렬키로 사용함
            elif message_body['state']=='after':
                print("after:",message_body)
                message_body['time']=time_sort_key
                
                # db check 작업 > 해당 유저가 story_event에 존재하지 않으면 삭제된 것!
                dynamodb_client=boto3.client('dynamodb')
                query=f"select * from Dy_story_event_{env} where user_id='{user_id}' and status='ongoing';"
                result=dynamodb_client.execute_statement(Statement=query)
                print(result)
                if len(result['Items'])==0:
                    print("회원 탈퇴한 유저!!")
                    return {
                        'statusCode': 200,
                        'body': json.dumps("bye!")
                    }
    
                temp=table_after_story.put_item(
                    Item=message_body
                )
                
                # 책이 모두 완성되었는지 확인!
                query=f"select in_dex from Dy_midjourney_output_story_upscale_{env} where user_id='{user_id}' and time_stamp='{time_stamp}';"
                result=dynamodb_client.execute_statement(Statement=query)
                
                # 책이 모두 완성됨!
                print(len(result["Items"]))
                if len(result["Items"])==8:
                    # sqs 전송 (SQS_make_book)
                    try:
                        # 최종 처리는 sqs에 연결된 lambda가 진행
                        sqs = boto3.resource('sqs', region_name='ap-northeast-2')
                        queue = sqs.get_queue_by_name(QueueName=f"SQS_make_book_{env}")
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
            
    except:
        print("put_dynamodb fail!!!!!")    

    print("Good!")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
