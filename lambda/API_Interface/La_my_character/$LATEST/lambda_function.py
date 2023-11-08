import json
import boto3
from presigned_url import generate_presigned_url
# La_my_character


def lambda_handler(event, context):
    
    dynamodb_client=boto3.client('dynamodb')

    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]
    print(env)

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
        try:
            # get image!
            query=f"select * from Dy_user_character_{env} where user_id='{user_id}' and state!='delete'"
            result=dynamodb_client.execute_statement(Statement=query)
            print(len(result["Items"]))
            
            # time_stamp를 key로 사용!
            bodyData={}
            
            for item in result["Items"]:
                time_stamp=item['time_stamp']['S']
                img_url=item['img_url']['S']
                cloth=item['cloth']['S']
                age=item['age']['N']
                name=item['name']['S']
                style=item['style']['S']
                gender=item['gender']['S']
                bodyData[f'{time_stamp}']={"img_url":img_url,"cloth":cloth,"age":age,"name":name,"style":style,"gender":gender}
            
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

    # 특정 캐릭터 이름 수정 (user_id, time_stamp 필요!!!)
    elif httpMethod=="POST" and path.split('/')[-1]=="image":
        try:
            item=event['body']
            item=json.loads(item)
            time_stamp=str(item['time_stamp'])
            name=str(item['name'])
            
            query=f"update Dy_user_character_{env} SET name='{name}' where user_id='{user_id}' and time_stamp='{time_stamp}'"
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
        print("delete")
        try:
            item=event['body']
            item=json.loads(item)
            time_stamp=str(item['time_stamp'])
            # query=f"delete from Dy_user_character_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            # update Dy_user_character_prod set state='delete' where user_id='mARK5mfyZLgtoVOGzCnFqcw4AQB3' and time_stamp='1690788405'
            query=f"update Dy_user_character_prod set state='delete' where user_id='{user_id}' and time_stamp='{time_stamp}'"
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
