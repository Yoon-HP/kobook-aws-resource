import json
import boto3
import time
from presigned_url import generate_presigned_url

def lambda_handler(event, context):
    
    # test code 확인
    # print(event)
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]
    
    dynamodb_client=boto3.client('dynamodb')
    print("hi")
    statusCode=200
    try:
        httpMethod=event['httpMethod']
        path=event["path"]
    except:
        print("firebase user!!")
    try:
        httpMethod=event['routeKey'].split()[0]
        path=event["rawPath"]
    except:
        print("cognito user!!")
    
    
    # cognito
    try:
        user_id=event['requestContext']['authorizer']['claims']['cognito:username']
    except:
        print("firebase user!!")
    
    # firebase
    try:
        user_id=event['requestContext']['authorizer']['jwt']['claims']['user_id']
    except:
        print("cognito user!!")
    
    print(user_id)
    if httpMethod=="GET" and path.split('/')[-1]=="book":
        # serach dynamodb
        env="prod"
        print(user_id)
        # return presigned url!!!
        try:
            # get image! << 이전 책들에 대한 소급적용 진행 해야함. < 코드 상에서 진행
            start_time=time.time()
            query=f"select * from Dy_user_book_{env} where user_id='{user_id}'"
            result=dynamodb_client.execute_statement(Statement=query)
            end_time=time.time()
            # 0.04s
            print(f"{end_time - start_time:.5f}")
            #print(query)
            #print(len(result["Items"]))
            
            # time_stamp를 key로 사용!
            bodyData={}
            
            s3_client=boto3.client('s3')
            bucket="s3-kkobook-story-image"
            client_action="get_object"
            
            for item in result["Items"]:
                time_stamp=item['time_stamp']['S']
                title=item['title']['S']
                num=item['num']['S']
                status=item['status']['S']
                temp={}
                
                # 아직 만들어지고 있는 책은 스킵
                if status=='ongoing':
                    temp['num']=num
                    bodyData[f'{time_stamp}']=temp
                    continue
                    
                temp['title']=title
                temp['num']=num
                # 새롭게 추가되는 부분
                temp['user_id']=user_id
                
                start_time=time.time()
                # text 불러오는 부분!
                #dynamodb_client=boto3.client('dynamodb')
                # 세션을 초기화 하는 경우 0.05s 세션이 유지되는 경우 0.005s
                query=f"SELECT * FROM Dy_gpt_story_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
                #print(query)
                result_story=dynamodb_client.execute_statement(Statement=query)
                    
                end_time=time.time()
                print(f"1: {end_time - start_time:.5f}")
                
                # result_story["Items"][0][story_keys[page_num]]['S']
                
                start_time=time.time()
                # 여기 부분이 새롭게 갱신 됨
                #dynamodb_client=boto3.client('dynamodb')
                # print("check!!")
                query=f"select * from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result_presigned_url=dynamodb_client.execute_statement(Statement=query)
                # print(result_presigned_url["Items"][0].keys())
                    
                end_time=time.time()
                print(f"2: {end_time - start_time:.5f}")
                
                for i in range(1,9):
                    # presigned_url, text
                    temp[str(i)]=[result_presigned_url["Items"][0][str(i)]['S'],result_story["Items"][0][str(i)]['S']]
        

                bodyData[f'{time_stamp}']=temp
            #print(bodyData)
            
            jsonData = json.dumps(bodyData, ensure_ascii=False).encode('utf8')
            #print(jsonData)
            return {
                'statusCode': 200,
                'body': jsonData
            }
                    
        except:
            statusCode=400
            return {
                'statusCode': statusCode,
                'body': json.dumps('Bad Request')
            }
            
    # 공유책보기 < 1페이지만 << api gateway 캐시 알아 X
    elif httpMethod=="GET" and path.split('/')[-1]=="shared":
        # serach dynamodb
        env="prod"
        # return presigned url!!!
        print("hi?")
        try:
            query=f"select * from Dy_user_book_{env}"
            result=dynamodb_client.execute_statement(Statement=query)
            #print(len(result["Items"]))
            
            # time_stamp를 key로 사용!
            bodyData={}
            
            s3_client=boto3.client('s3')
            bucket="s3-kkobook-story-image"
            client_action="get_object"
            
            for item in result["Items"]:
                user_id=item['user_id']['S']
                time_stamp=item['time_stamp']['S']
                title=item['title']['S']
                num=item['num']['S']
                status=item['status']['S']
                temp={}
                
                # 아직 만들어지고 있는 책은 스킵
                if status=='ongoing':
                    temp['num']=num
                    bodyData[f'{time_stamp}']=temp
                    continue
                    
                temp['title']=title
                temp['num']=num
                # << 향후 식별을 위함
                temp['user_id']=user_id
                
                
                # result_story["Items"][0][story_keys[page_num]]['S']
                
                
                # 여기 부분이 새롭게 갱신 됨 << 1페이지만 return!
                # dynamodb_client=boto3.client('dynamodb')
                #print("check!!")
                query=f"select \"1\" from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result_presigned_url=dynamodb_client.execute_statement(Statement=query)
                # print(result_presigned_url["Items"][0].keys())
                
                temp['1']=result_presigned_url["Items"][0]['1']['S']

                bodyData[f'{time_stamp}']=temp
            #print(bodyData)
            
            jsonData = json.dumps(bodyData, ensure_ascii=False).encode('utf8')
            #print(jsonData)
            print(len(jsonData))
            return {
                'statusCode': 200,
                'body': jsonData
            }
                    
        except:
            statusCode=400
            return {
                'statusCode': statusCode,
                'body': json.dumps('Bad Request')
            }
        

    # 책 상세보기
    elif httpMethod=="GET" and path.split('/')[-1]=="detail":
        # serach dynamodb
        env="prod"
        print(user_id)
        # return presigned url!!!
        try:
            query=f"select * from Dy_user_book_{env} where user_id='{user_id}'"
            result=dynamodb_client.execute_statement(Statement=query)
            #print(query)
            #print(len(result["Items"]))
            
            # time_stamp를 key로 사용!
            bodyData={}
            
            s3_client=boto3.client('s3')
            bucket="s3-kkobook-story-image"
            client_action="get_object"
            
            for item in result["Items"]:
                time_stamp=item['time_stamp']['S']
                title=item['title']['S']
                num=item['num']['S']
                status=item['status']['S']
                temp={}
                
                # 아직 만들어지고 있는 책은 스킵
                if status=='ongoing':
                    temp['num']=num
                    bodyData[f'{time_stamp}']=temp
                    continue
                    
                temp['title']=title
                temp['num']=num
                # 새롭게 추가되는 부분
                temp['user_id']=user_id
                
                # text 불러오는 부분!
                dynamodb_client=boto3.client('dynamodb')
                query=f"SELECT * FROM Dy_gpt_story_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
                #print(query)
                result_story=dynamodb_client.execute_statement(Statement=query)
                
                # result_story["Items"][0][story_keys[page_num]]['S']
                
                
                # 여기 부분이 새롭게 갱신 됨
                dynamodb_client=boto3.client('dynamodb')
                # print("check!!")
                query=f"select * from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result_presigned_url=dynamodb_client.execute_statement(Statement=query)
                # print(result_presigned_url["Items"][0].keys())
                
                for i in range(1,9):
                    # presigned_url, text
                    temp[str(i)]=[result_presigned_url["Items"][0][str(i)]['S'],result_story["Items"][0][str(i)]['S']]
        

                bodyData[f'{time_stamp}']=temp
            #print(bodyData)
            
            jsonData = json.dumps(bodyData, ensure_ascii=False).encode('utf8')
            print(jsonData)
            return {
                'statusCode': 200,
                'body': jsonData
            }
                    
        except:
            statusCode=400
            return {
                'statusCode': statusCode,
                'body': json.dumps('Bad Request')
            }

    # 책 타이틀 수정 (user_id, time_stamp, title 필요!!!)
    elif httpMethod=="POST" and path.split('/')[-1]=="book":
        try:
            # 책이 만들어진 시점의 time_stamp만 있으면 됨
            item=event['body']
            item=json.loads(item)
            time_stamp=str(item['time_stamp'])
            title=str(item['title'])
            
            query=f"update Dy_user_book_{env} SET title='{title}' where user_id='{user_id}' and time_stamp='{time_stamp}'"
            result=dynamodb_client.execute_statement(Statement=query)
        
            return {
                'statusCode': 200,
                'body': json.dumps('Name update success')
            }
            
        except:
            statusCode=400
            return {
                'statusCode': statusCode,
                'body': json.dumps('Bad Request')
            }
    
    # 특정 책 삭제! (user_id, time_stamp 필요!!)
    elif httpMethod=="POST" and path.split('/')[-1]=="delete":
        try:
            item=event['body']
            item=json.loads(item)
            time_stamp=str(item['time_stamp'])
            query=f"delete from Dy_user_book_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            result=dynamodb_client.execute_statement(Statement=query)
            
            return {
                'statusCode': 200,
                'body': json.dumps('Delete success')
            }
            
        except:
            statusCode=400
            return {
                'statusCode': statusCode,
                'body': json.dumps('Bad Request')
            }
    
    return {
        'statusCode': 405,
        'body': json.dumps('Method Not Allowed')
    }
