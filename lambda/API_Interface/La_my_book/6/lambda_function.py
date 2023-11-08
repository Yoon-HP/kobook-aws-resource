import json
import boto3
import time
from presigned_url import generate_presigned_url


def lambda_handler(event, context):
    # test code 확인
    # print(event)
    # version check
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
    if httpMethod == "GET" and path.split("/")[-1] == "book":
        # serach dynamodb
        env = "prod"
        print(user_id)
        # return presigned url!!!
        try:
            # get image! << 이전 책들에 대한 소급적용 진행 해야함. < 코드 상에서 진행
            start_time = time.time()
            query = f"select * from Dy_user_book_{env} where user_id='{user_id}'"
            result = dynamodb_client.execute_statement(Statement=query)
            end_time = time.time()
            # 0.04s
            print(f"{end_time - start_time:.5f}")
            # print(query)
            # print(len(result["Items"]))

            # time_stamp를 key로 사용!
            bodyData = {}

            for item in result["Items"]:
                time_stamp = item["time_stamp"]["S"]
                title = item["title"]["S"]
                num = item["num"]["S"]
                status = item["status"]["S"]
                temp = {}

                # 아직 만들어지고 있는 책은 스킵
                if status == "ongoing":
                    temp["num"] = num
                    bodyData[f"{time_stamp}"] = temp
                    continue

                temp["title"] = title
                temp["num"] = num
                # 새롭게 추가되는 부분
                temp["user_id"] = user_id

                # start_time = time.time()

                """
                # text 불러오는 부분!
                #dynamodb_client=boto3.client('dynamodb')
                # 세션을 초기화 하는 경우 0.05s 세션이 유지되는 경우 0.005s
                query=f"SELECT * FROM Dy_gpt_story_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
                #print(query)
                result_story=dynamodb_client.execute_statement(Statement=query)
                    
                end_time=time.time()
                print(f"1: {end_time - start_time:.5f}")
                """
                # result_story["Items"][0][story_keys[page_num]]['S']

                # start_time = time.time()
                # 여기 부분이 새롭게 갱신 됨
                # dynamodb_client=boto3.client('dynamodb')
                # print("check!!")
                query = f"select \"1\" from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result_presigned_url = dynamodb_client.execute_statement(
                    Statement=query
                )
                # print(result_presigned_url["Items"][0].keys())

                temp["1"] = result_presigned_url["Items"][0]["1"]["S"]

                # end_time = time.time()
                # print(f"2: {end_time - start_time:.5f}")

                bodyData[f"{time_stamp}"] = temp
            # print(bodyData)
            print(len(bodyData))

            jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
            # print(jsonData)
            return {"statusCode": 200, "body": jsonData}

        except:
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}

    # 공유책보기 < 1페이지만 << api gateway 캐시 알아 X
    elif httpMethod == "GET" and path.split("/")[-1] == "shared":
        # serach dynamodb
        env = "prod"
        # return presigned url!!!
        print("hi?")
        try:
            query = f"select * from Dy_user_book_{env}"
            result = dynamodb_client.execute_statement(Statement=query)
            # print(len(result["Items"]))

            # time_stamp를 key로 사용!
            bodyData = {}

            for item in result["Items"]:
                user_id = item["user_id"]["S"]
                time_stamp = item["time_stamp"]["S"]
                title = item["title"]["S"]
                num = item["num"]["S"]
                status = item["status"]["S"]
                temp = {}

                # 아직 만들어지고 있는 책은 스킵
                if status == "ongoing":
                    # temp['num']=num
                    # bodyData[f'{time_stamp}']=temp
                    continue

                temp["title"] = title
                temp["num"] = num
                # << 향후 식별을 위함
                temp["user_id"] = user_id

                # result_story["Items"][0][story_keys[page_num]]['S']

                # 여기 부분이 새롭게 갱신 됨 << 1페이지만 return!
                # dynamodb_client=boto3.client('dynamodb')
                # print("check!!")
                query = f"select \"1\" from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result_presigned_url = dynamodb_client.execute_statement(
                    Statement=query
                )
                # print(result_presigned_url["Items"][0].keys())

                temp["1"] = result_presigned_url["Items"][0]["1"]["S"]

                bodyData[f"{time_stamp}"] = temp
            # print(bodyData)

            temp = {}

            temp_list = list(bodyData.keys())
            temp_list.sort()

            for tp in temp_list:
                temp[tp] = bodyData[tp]

            print(len(temp))
            jsonData = json.dumps(temp, ensure_ascii=False).encode("utf8")
            # print(jsonData)
            print(len(jsonData))
            return {"statusCode": 200, "body": jsonData}

        except:
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}

    # 책 상세보기 << 개발할게 좀 있음
    elif httpMethod == "POST" and path.split("/")[-1] == "detail":
        # serach dynamodb
        env = "prod"
        try:
            # 책이 만들어진 시점의 time_stamp만 있으면 됨
            item = event["body"]
            item = json.loads(item)
            time_stamp = str(item["time_stamp"])
            user_id = str(item["user_id"])
            print(user_id)
            print(time_stamp)
        except:
            print("something wrong!")
        # return presigned url!!!
        try:
            query = f"select * from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
            result = dynamodb_client.execute_statement(Statement=query)

            # print("ck1")

            query = f"SELECT * FROM Dy_gpt_story_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            # print(query)
            result_story = dynamodb_client.execute_statement(Statement=query)

            query = f"SELECT * FROM Dy_gpt_story_english_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            # print(query)
            result_story_eng = dynamodb_client.execute_statement(Statement=query)


            # print("ck2")
            # time_stamp를 key로 사용!
            bodyData = {}

            for i in range(1, 9):
                
                temp=""
                for check in result_story["Items"][0][str(i)]["S"]:
                    if check=="|":
                        temp+="'"
                    else:
                        temp+=check
                
                # 작은 따움표 치환 작업 진행
                '''
                bodyData[str(i)] = [
                    result["Items"][0][str(i)]["S"],
                    result_story["Items"][0][str(i)]["S"],
                ]
                '''
                bodyData[str(i)] = [
                    result["Items"][0][str(i)]["S"],
                    temp,
                    result_story_eng["Items"][0][str(i)]["S"],
                ]
            # print("ck3")

            query = f"SELECT * FROM Dy_social_book_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
            # print(query)
            result_social = dynamodb_client.execute_statement(Statement=query)
            print(result_social)
            like_num = result_social["Items"][0]["like_num"]["N"]
            bodyData["like_num"] = like_num

            # print("ck4")

            query = f"SELECT * FROM Dy_story_summary_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
            # print(query)
            result_summary = dynamodb_client.execute_statement(Statement=query)
            summary = result_summary["Items"][0]["summary"]["S"]
            bodyData["summary"] = summary

            # print("ck5")

            jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
            print(jsonData)
            return {"statusCode": 200, "body": jsonData}

        except:
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}

    # 책 상태보기
    elif httpMethod == "GET" and path.split("/")[-1] == "status":
        # serach dynamodb
        env = "prod"
        print(user_id)
        # 현재 해당 유저가 만들고 있는 책이 존재하는지 파악
        try:
            query = f"select * from Dy_user_book_{env} where user_id='{user_id}' and status='ongoing'"
            result = dynamodb_client.execute_statement(Statement=query)
            print(result)
            try:
                time_stamp = result["Items"][0]["time_stamp"]["S"]
            except:
                print("Hi!!")

            temp = {}

            # ongoing 상태라면 1,
            # print("ck!!")
            # 만들고 있는 책이 존재하는 경우
            if len(result["Items"]):
                pk = f"s_t/{user_id}/{time_stamp}/1/0"
                print(pk)
                # print("ck!!")
                try:
                    query = (
                        f"select * from Dy_midjourney_check_story_prod where pk='{pk}'"
                    )
                    result_check = dynamodb_client.execute_statement(Statement=query)
                    print(f"result_check: {result_check}")
                except:
                    print("query fail!")
                temp["status"] = "1"
                if len(result_check["Items"]):
                    check = result_check["Items"][0]["check"]["S"]
                    if check == "no" or check == "yes":
                        # 미드저니에서 작업이 진행되고 있는 경우 3 < no or yes 상태
                        temp["status"] = "3"
                    else:
                        # 미드저니에 보낼 준비가 완료된 상황 < start 상태
                        temp["status"] = "2"
                else:
                    # 미드저니에 요청이 들어가지 않은 경우 1 <<
                    temp["status"] = "1"

                print("ck!!")

                query = f"select * from Dy_user_book_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result_ck = dynamodb_client.execute_statement(Statement=query)
                print(f"result_ck: {result_ck}")
                status = result_ck["Items"][0]["status"]["S"]
                if status == "finish":
                    # 미드저니에서 모든 작업이 끝난 경우 3
                    temp["status"] = "4"
                print("ck!!")
            else:
                print("no book!")
                # 만들고 있는 책이 없는 경우
                temp["status"] = "4"

            jsonData = json.dumps(temp, ensure_ascii=False).encode("utf8")
            print(jsonData)
            return {"statusCode": 200, "body": jsonData}
        except:
            print("??")
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}

    # 책 reroll 요청!
    elif httpMethod == "POST" and path.split("/")[-1] == "reroll":
        env = "prod"
        try:
            # 책이 만들어진 시점의 time_stamp만 있으면 됨
            item = event["body"]
            item = json.loads(item)
            time_stamp = str(item["time_stamp"])
            user_id = str(item["user_id"])
            page_num = str(item["page_num"])
            print(user_id)
            print(time_stamp)
            print(page_num)
        except:
            print("something wrong!")
        # return presigned url!!!
        try:
            
            start_time=time.time()
            
            query = f"select * from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
            result = dynamodb_client.execute_statement(Statement=query)
            page_index=result["Items"][0]["page_index"]['S']
            
            
            end_time = time.time()
            print(f"{end_time - start_time:.5f}")
            
            # str -> list
            cur_index_list=list(page_index)
            
            
            # pageindex 갱신 작업 진행
            cur_page_index=int(cur_index_list[int(page_num)-1])
            if cur_page_index==4:
                cur_index_list[int(page_num)-1]='1'
            else:
                cur_index_list[int(page_num)-1]=str(cur_page_index+1)
            
            updated_page_index="".join(cur_index_list)
            
            s3_client=boto3.client('s3')
            bucket_pre="s3-kkobook-story-image"
            client_action="get_object"
            
            
            start_time=time.time()
            
            key=f"upscale/{user_id}/{time_stamp}/{int(page_num)}/{updated_page_index[int(page_num)-1]}.jpg"
            url = generate_presigned_url(s3_client, client_action, {'Bucket': bucket_pre, 'Key': key}, 604800)

            # 업데이트 내용 반영
            
            #dynamodb_client=boto3.client('dynamodb')
            # page_index 갱신
            query=f"UPDATE Dy_story_image_prod SET page_index = '{updated_page_index}' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
            result=dynamodb_client.execute_statement(Statement=query)

            # url 갱신
            query=f"UPDATE Dy_story_image_prod SET \"{page_num}\" = '{url}' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
            result=dynamodb_client.execute_statement(Statement=query)
                      
            end_time = time.time()
            print(f"{end_time - start_time:.5f}")      
                             
            # 새로운 presigned_url return
            bodyData = {}
            
            bodyData["img_url"]=url
            

            jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
            print(jsonData)
            return {"statusCode": 200, "body": jsonData}

        except:
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}

    # 책 타이틀 수정 (user_id, time_stamp, title 필요!!!)
    elif httpMethod == "POST" and path.split("/")[-1] == "book":
        try:
            # 책이 만들어진 시점의 time_stamp만 있으면 됨
            item = event["body"]
            item = json.loads(item)
            time_stamp = str(item["time_stamp"])
            title = str(item["title"])

            query = f"update Dy_user_book_{env} SET title='{title}' where user_id='{user_id}' and time_stamp='{time_stamp}'"
            result = dynamodb_client.execute_statement(Statement=query)

            return {"statusCode": 200, "body": json.dumps("Name update success")}

        except:
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}


    # 책 동화수정 (user_id,time_stamp,page_num,text)
    elif httpMethod == "POST" and path.split("/")[-1] == "text":
        env = "prod"
        try:
            # 책이 만들어진 시점의 time_stamp만 있으면 됨
            item = event["body"]
            item = json.loads(item)
            time_stamp = str(item["time_stamp"])
            user_id = str(item["user_id"])
            page_num = str(item["page_num"])
            text=str(item["text"])
            
            print(user_id)
            print(time_stamp)
            print(page_num)
            print(text)
        except:
            print("something wrong!")
        
        temp=""
        for check in text:
            if check=="'":
                temp+="|"
                continue
            temp+=check
        
        text=temp
        
        query="update Dy_gpt_story_%(env)s set \"%(page_num)s\"='%(text)s' where user_id='%(user_id)s' and time_stamp='%(time_stamp)s'" % {"env":env,"page_num":page_num,"text":text,"user_id":user_id,"time_stamp":time_stamp,}
        #query=f"update Dy_gpt_story_{env} set \"{page_num}\"='{text}' where user_id='{user_id}' and time_stamp='{time_stamp}'"
        print(query)
        result=dynamodb_client.execute_statement(Statement=query)
        print("text update 완료!")

        return {"statusCode": 200, "body": json.dumps("text update success!")}

    # 특정 책 삭제! (user_id, time_stamp 필요!!)
    elif httpMethod == "POST" and path.split("/")[-1] == "delete":
        try:
            item = event["body"]
            item = json.loads(item)
            time_stamp = str(item["time_stamp"])
            query = f"delete from Dy_user_book_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            result = dynamodb_client.execute_statement(Statement=query)

            return {"statusCode": 200, "body": json.dumps("Delete success")}

        except:
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}
            
    # 퀴즈 요청
    elif httpMethod == "POST" and path.split("/")[-1] == "quiz":
        env = "prod"
        try:
            # 책이 만들어진 시점의 time_stamp만 있으면 됨
            item = event["body"]
            item = json.loads(item)
            time_stamp = str(item["time_stamp"])
            user_id = str(item["user_id"])
            print(user_id)
            print(time_stamp)
        except:
            print("something wrong!")
        
        # 퀴즈 parsing 진행
        try:
            query = f"select * from Dy_story_quiz_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
            result = dynamodb_client.execute_statement(Statement=query)
            print(result)
            
            choices_1={}
            choices_2={}
            choices_3={}
            choices_4={}
            
            choices_1_temp=result["Items"][0]['1_choices']['L']
            choices_2_temp=result["Items"][0]['2_choices']['L']
            choices_3_temp=result["Items"][0]['3_choices']['L']
            choices_4_temp=result["Items"][0]['4_choices']['L']
            
            for i in range(4):
                choices_1[str(i+1)]=choices_1_temp[i]['S']
                choices_2[str(i+1)]=choices_2_temp[i]['S']
                choices_3[str(i+1)]=choices_3_temp[i]['S']
                choices_4[str(i+1)]=choices_4_temp[i]['S']
            
            
            bodyData={}
            
            for i in range(4):
                bodyData[f"correct_answer_{i+1}"]=result["Items"][0][f'{i+1}_correct_answer']['S']
                bodyData[f"question_{i+1}"]=result["Items"][0][f'{i+1}_question']['S']
            
            bodyData['choices_1']=choices_1
            bodyData['choices_2']=choices_2
            bodyData['choices_3']=choices_3
            bodyData['choices_4']=choices_4
        
            jsonData = json.dumps(bodyData, ensure_ascii=False).encode("utf8")
            # print(jsonData)
            
            return {"statusCode": 200, "body": jsonData}
        except:
            print("quiz issue!")
            # lambda invoke 부분 추가
            try:
                lambda_client=boto3.client('lambda')
                
                # type에 에러 위치  명명
                payload={
                    'user_id':user_id,
                    'time_stamp':time_stamp,
                    'type':"quiz"
                }
                response = lambda_client.invoke(
                    FunctionName='La_api_issue_handling',
                    InvocationType='Event',
                    Payload=json.dumps(payload)
                )
            except:
                print("lambda invoke fail!")
            
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}
            

    return {"statusCode": 405, "body": json.dumps("Method Not Allowed")}
