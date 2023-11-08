import json
import boto3

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
        try:
            # get image!
            query=f"select * from Dy_user_character where user_id='{user_id}'"
            result=dynamodb_client.execute_statement(Statement=query)
            print(len(result["Items"]))
            
            # datetime을 key로 사용!
            bodyData={}
            
            for item in result["Items"]:
                datetime=item['datetime']['S']
                img_url=item['img_url']['S']
                cloth=item['cloth']['S']
                age=item['age']['N']
                name=item['name']['S']
                style=item['style']['S']
                gender=item['gender']['S']
                bodyData[f'{datetime}']={"img_url":img_url,"cloth":cloth,"age":age,"name":name,"style":style,"gender":gender}
            
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

    # 특정 캐릭터 이름 수정 (user_id, datetime 필요!!!)
    elif httpMethod=="POST" and path.split('/')[-1]=="image":
        try:
            item=event['body']
            item=json.loads(item)
            datetime=str(item['datetime'])
            name=str(item['name'])
            
            query=f"update Dy_user_character SET name='{name}' where user_id='{user_id}' and datetime='{datetime}'"
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
    
    # 특정 캐릭터 삭제! (user_id, datetime 필요!!)
    elif httpMethod=="POST" and path.split('/')[-1]=="delete":
        print("delete")
        try:
            item=event['body']
            item=json.loads(item)
            datetime=str(item['datetime'])
            query=f"delete from Dy_user_character where user_id='{user_id}' and datetime='{datetime}'"
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
