import json
import boto3
import time
from presigned_url import generate_presigned_url


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
        
        
    # global인지 아닌지 파악
    try:
        item=event['body']
        item=json.loads(item)
        ver=item['ver']
    except:
        ver=""        

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
    elif httpMethod == "GET" and path.split("/")[-1] == "coin":

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


    # 동화 option 변경 API
    elif httpMethod == "POST" and path.split("/")[-1] == "option":

        env = "prod"
        try:
            # 책이 만들어진 시점의 time_stamp만 있으면 됨
            item = event["body"]
            item = json.loads(item)
            time_stamp = str(item["time_stamp"])
            # user_id = str(item["user_id"])
            check=str(item["check"])
            
            #print(user_id)
            print(time_stamp)
            print(check)
            
            # public -> private
            if check=="0":
                query=f"UPDATE Dy_user_book_prod SET \"option\" = 'private' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
                result=dynamodb_client.execute_statement(Statement=query)
            elif check=="1":
                # private -> public
                query=f"UPDATE Dy_user_book_prod SET \"option\" = 'public' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
                result=dynamodb_client.execute_statement(Statement=query)
            
            #bodyData={}
            # 정수형으로 리턴
            #bodyData['coin_num']=coin
                
            #jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
            #print(jsonData)
            return {"statusCode": 200, "body": json.dumps('Hello from Lambda!')}
            
        except:
            print("something wrong!")


    # 좋아요 랭킹
    elif httpMethod == "GET" and path.split("/")[-1] == "ranking":

        env = "prod"
        print("Hi")
        print(ver)
        try:
            if ver!="global":
                # user id로 쿼리
                query = f"select * from Dy_social_book_prod"
                result = dynamodb_client.execute_statement(Statement=query)
                
                sorting_temp=[]
                for item in result["Items"]:
                    user_id=item['user_id']['S']
                    time_stamp=item['time_stamp']['S']
                    like_num=item['like_num']['N']
                    
                    sorting_temp.append([like_num,time_stamp,user_id])
                
                # 역순 정렬
                sorting_temp.sort(reverse=True)
                #print(sorting_temp)
                
                # 바뀔 수 있음 << private은 포함 X
                rank=5
                
                bodyData={}
                
                cnt=0
                in_dex=-1
                while cnt<rank:
                    
                    in_dex+=1
                    
                    like_num=sorting_temp[in_dex][0]
                    user_id=sorting_temp[in_dex][2]
                    time_stamp=sorting_temp[in_dex][1]
    
                    # title, num, option
                    query_tp = f"select * from Dy_user_book_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                    result = dynamodb_client.execute_statement(Statement=query_tp)
                    
                    item=result["Items"][0]
                    title = item["title"]["S"]
                    num = item["num"]["S"]
                    status = item["status"]["S"]
                    # 공개 여부
                    option = item['option']['S']
    
                    try:
                        # global 버전은 제외
                        if item["ver"]["S"]=="global":
                            flag=True
                        else:
                            flag=False
                    except:
                        flag=False
    
                    # 아직 만들어지고 있는 책은 스킵
                    if status == "ongoing" or option == "private":
                        # temp['num']=num
                        # bodyData[f'{time_stamp}']=temp
                        continue
                    
                    if flag:
                        continue        
                    
                    temp={}
                    temp["title"] = title
                    temp["num"] = num
                    temp['like_num']=like_num
                    
                    # << 향후 식별을 위함
                    temp["user_id"] = user_id
    
                    query = f"select \"1\" from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                    result_presigned_url = dynamodb_client.execute_statement(
                        Statement=query
                    )
                    # print(result_presigned_url["Items"][0].keys())
    
                    temp["1"] = result_presigned_url["Items"][0]["1"]["S"]
                    
                    bodyData[f"{time_stamp}"] = temp
                    cnt+=1
                
                jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
                print(jsonData)
                return {"statusCode": 200, "body": jsonData}
            
            else:
                # global!    
                query = f"select * from Dy_global_user_book"
                result = dynamodb_client.execute_statement(Statement=query)
                
                sorting_temp=[]
                for item in result["Items"]:
                    user_id=item['user_id']['S']
                    time_stamp=item['time_stamp']['S']
                    
                    query_t = f"select * from Dy_social_book_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                    result_li = dynamodb_client.execute_statement(Statement=query_t)
                    
                    like_num=result_li["Items"][0]['like_num']['S']
                    
                    sorting_temp.append([like_num,user_id,time_stamp])
                
                # 역순 정렬
                sorting_temp.sort(reverse=True)
                
                # 바뀔 수 있음 << private은 포함 X
                rank=5
                
                bodyData={}
                
                cnt=0
                in_dex=-1
                while cnt<rank:
                    
                    in_dex+=1
                    
                    like_num=sorting_temp[in_dex][0]
                    user_id=sorting_temp[in_dex][1]
                    time_stamp=sorting_temp[in_dex][2]
    
                    # title, num, option
                    query_tp = f"select * from Dy_user_book_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                    result = dynamodb_client.execute_statement(Statement=query_tp)
                    
                    item=result["Items"][0]
                    title = item["title"]["S"]
                    num = item["num"]["S"]
                    status = item["status"]["S"]
                    # 공개 여부
                    option = item['option']['S']
    
                    # 아직 만들어지고 있는 책은 스킵
                    if status == "ongoing" or option == "private":
                        # temp['num']=num
                        # bodyData[f'{time_stamp}']=temp
                        continue

                    temp={}
                    temp["title"] = title
                    temp["num"] = num
                    temp['like_num']=like_num
                    
                    # << 향후 식별을 위함
                    temp["user_id"] = user_id
    
                    query = f"select \"1\" from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                    result_presigned_url = dynamodb_client.execute_statement(
                        Statement=query
                    )
                    # print(result_presigned_url["Items"][0].keys())
    
                    temp["1"] = result_presigned_url["Items"][0]["1"]["S"]
                    
                    bodyData[f"{time_stamp}"] = temp
                    cnt+=1
                
                jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
                print(jsonData)
                return {"statusCode": 200, "body": jsonData}
                            

        except:
            print("something wrong!")


        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
