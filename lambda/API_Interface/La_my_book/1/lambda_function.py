import json
import boto3
from presigned_url import generate_presigned_url

# La_my_character


def lambda_handler(event, context):
    
    dynamodb_client=boto3.client('dynamodb')

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
        
    if httpMethod=="GET":
        # serach dynamodb
        
        # return presigned url!!!
        try:
            # get image!
            query=f"select * from Dy_user_book where user_id='{user_id}'"
            result=dynamodb_client.execute_statement(Statement=query)
            print(len(result["Items"]))
            
            # time_stamp를 key로 사용!
            bodyData={}
            
            s3_client=boto3.client('s3')
            bucket="s3-kkobook-book"
            client_action="get_object"
            
            for item in result["Items"]:
                time_stamp=item['time_stamp']['S']
                title=item['title']['S']
                temp={}

                temp['title']=title
                
                #thumbnail
                try:
                    key=f"{user_id}/{time_stamp}/thumbnail.png"
                    url = generate_presigned_url(s3_client, client_action, {'Bucket': bucket, 'Key': key}, 3600)
                    temp["thumbnail"]=url
                except:
                    print("thumbnail not exist!")
                
                # get presigned url 현재 8 page 고정
                for page in range(1,9):
                    key=f"{user_id}/{time_stamp}/{page}.png"
                    url = generate_presigned_url(s3_client, client_action, {'Bucket': bucket, 'Key': key}, 3600)
                    temp[str(page)]=url
                
                bodyData[f'{time_stamp}']=temp
                
            print(bodyData)
            
            jsonData = json.dumps(bodyData, ensure_ascii=False).encode('utf8')
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
            
            query=f"update Dy_user_book SET title='{title}' where user_id='{user_id}' and time_stamp='{time_stamp}'"
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
    
    # 특정 캐릭터 삭제! (user_id, time_stamp 필요!!)
    elif httpMethod=="POST" and path.split('/')[-1]=="delete":
        try:
            item=event['body']
            item=json.loads(item)
            time_stamp=str(item['time_stamp'])
            query=f"delete from Dy_user_book where user_id='{user_id}' and time_stamp='{time_stamp}'"
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
