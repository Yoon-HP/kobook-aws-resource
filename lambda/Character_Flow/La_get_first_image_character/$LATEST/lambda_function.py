import json
import boto3

#La_get_first_image_character

# 캐릭터 이미지가 생성되었을 경우 이를 client에게 return 해주는 함수 
# path: /character/get_first_image



def lambda_handler(event, context):
    
    dynamodb_client=boto3.client('dynamodb')
    # 상태 코드
    #print(event)
    statusCode=200

    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]    

    # serach dynamodb
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
            
        if httpMethod!="GET":
            statusCode=405
            return {
                'statusCode': statusCode,
                'body': json.dumps('Method Not Allowed')
            }
            
        #print(user_id)
        # get datetime
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
        print(user_id,time_stamp)
    except:
        statusCode=400
        return {
            'statusCode': statusCode,
            'body': json.dumps('Bad Request')
        }
    
    # 만약 face_fail이 발생한 경우 500 code return!
    try:
        query=f"select status from Dy_character_event_{env} where user_id='{user_id}' and fail='face_fail' and time_stamp='{time_stamp}'"
        result=dynamodb_client.execute_statement(Statement=query)
        if len(result["Items"])!=0:
            statusCode=500
            print ("face_fail!!!")
            
            bodyData={"reason":'face_fail'}
            jsonData = json.dumps(bodyData, ensure_ascii=False).encode('utf8')
            return {
                'statusCode': statusCode,
                'body': jsonData
            }
    except:
        statusCode=500
        return {
            'statusCode': statusCode,
            'body': json.dumps('Internet Sever Error')
        }

    # 만약 image_fail이 발생한 경우 500 code return!
    try:
        query=f"select status from Dy_character_event_{env} where user_id='{user_id}' and fail='image_fail' and time_stamp='{time_stamp}'"
        result=dynamodb_client.execute_statement(Statement=query)
        if len(result["Items"])!=0:
            statusCode=500
            print ("image_fail!!!")
            bodyData={"reason":'image_fail'}
            jsonData = json.dumps(bodyData, ensure_ascii=False).encode('utf8')
            return {
                'statusCode': statusCode,
                'body': jsonData
            }
    except:
        statusCode=500
        return {
            'statusCode': statusCode,
            'body': json.dumps('Internet Sever Error')
        }
    
    # image가 생성되지 않았을 경우 503 code return
    try:
        # get first image url
        query=f"select img_url from Dy_midjourney_output_character_{env} where user_id='{user_id}' and state='before' and time_stamp='{time_stamp}'"
        result=dynamodb_client.execute_statement(Statement=query)
        print(result)
        if len(result["Items"])!=1:
            statusCode=503
            return {
                'statusCode': statusCode,
                'body': json.dumps('Service Unavailable')
            }
            
        img_url=result["Items"][0]['img_url']['S']
    except:
        statusCode=503
        return {
            'statusCode': statusCode,
            'body': json.dumps('Service Unavailable')
        }
    bodyData={"img_url":img_url}
    jsonData = json.dumps(bodyData, ensure_ascii=False).encode('utf8')
    
    print("Good!")
    return {
        'statusCode': statusCode,
        'body': jsonData
    }
