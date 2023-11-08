import json
import boto3
import time

def lambda_handler(event, context):

    function_arn = context.invoked_function_arn
    env = function_arn.split(":")[-1]

    dynamodb_client = boto3.client("dynamodb")
    print("hi")
    print(event)
    statusCode = 200
    try:
        httpMethod = event["httpMethod"]
        path = event["path"]
    except:
        print("firebase user!!")
    try:
        httpMethod = event["routeKey"].split()[0]
        path = event["rawPath"]
    except:
        print("cognito user!!")

    # cognito
    try:
        user_id = event["requestContext"]["authorizer"]["claims"]["cognito:username"]
    except:
        print("firebase user!!")

    # firebase
    try:
        user_id = event["requestContext"]["authorizer"]["jwt"]["claims"]["user_id"]
    except:
        print("cognito user!!")
        
        
    print(user_id)
    print(path)

    if httpMethod == "POST" and path.split("/")[-1] == "like":

        env = "prod"
        try:
            # 책이 만들어진 시점의 time_stamp만 있으면 됨
            item = event["body"]
            item = json.loads(item)
            time_stamp = str(item["time_stamp"])
            user_id = str(item["user_id"])
            check=str(item["check"])
            print(user_id)
            print(time_stamp)
            print(check)
            
            # 좋아요 누름
            if check=="0":
                
                # like_num 받아오기
                query = f"select * from Dy_social_book_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result = dynamodb_client.execute_statement(Statement=query)
                like_num=int(result["Items"][0]["like_num"]['N'])
                
                # like_num 받아오기 < 확인 필요
                query = f"update Dy_social_book_prod set like_num={like_num+1} where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result = dynamodb_client.execute_statement(Statement=query)
                
                bodyData={}
                bodyData['like_num']=like_num+1
                
                    
                jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
                print(jsonData)
                return {"statusCode": 200, "body": jsonData}
                
            else:

                # like_num 받아오기
                query = f"select * from Dy_social_book_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result = dynamodb_client.execute_statement(Statement=query)
                like_num=int(result["Items"][0]["like_num"]['N'])
                
                # like_num 받아오기 < 확인 필요
                
                # 음수가 되는 것 방지
                if like_num==0:
                    like_num+=1
                
                query = f"update Dy_social_book_prod set like_num={like_num-1} where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result = dynamodb_client.execute_statement(Statement=query)
                
                bodyData={}
                bodyData['like_num']=like_num-1
                
                    
                jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
                print(jsonData)
                return {"statusCode": 200, "body": jsonData}
                
        except:
            print("something wrong!")
            
    # coin 개수 확인
    if httpMethod == "GET" and path.split("/")[-1] == "coin":

        env = "prod"
        try:
            # user id로 쿼리
            query = f"select * from Dy_user_prod where user_id='{user_id}'"
            result = dynamodb_client.execute_statement(Statement=query)
            coin=int(result["Items"][0]["coin"]['S'])

            bodyData={}
            # 정수형으로 리턴
            bodyData['coin_num']=coin
            
                
            jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
            print(jsonData)
            return {"statusCode": 200, "body": jsonData}
            
        except:
            print("something wrong!")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
