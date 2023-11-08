import json
import boto3
import time
from datetime import datetime, timedelta, timezone
from presigned_url import generate_presigned_url
# 책 url 저장 db 생성
def lambda_handler(event, context):
    
    # 하루에 한번 갱신할거라 renew 시점을 저장할 필요는 없어보임
    '''
    # 시간
    datetime_utc = datetime.utcnow()
    timezone_kst = timezone(timedelta(hours=9))
    # 현재 한국 시간
    datetime_kst = datetime_utc.astimezone(timezone_kst)
    '''
    
    '''
    # 동화 renew
    try:
        # s3-kkobook-book
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('s3-kkobook-book')
        prefix = f""
        cnt=0
            
        s3_client=boto3.client('s3')
        bucket_pre="s3-kkobook-book"
        client_action="get_object"
        
        # dynamodb table에 넣을 형태를 만든 후 put 작업 진행
        temp_list=[]
        temp_dict={}
        prev_user_id=""
        prev_time_stamp=""
        for obj in bucket.objects.filter(Prefix=prefix):
            #print(obj.key)
            user_id=obj.key.split('/')[0]
            time_stamp=obj.key.split('/')[1]
            if user_id!=prev_user_id or time_stamp!=prev_time_stamp:
                #temp_dict['renew_time']=str(int(datetime_kst.timestamp()))
                temp_list.append(temp_dict)
                temp_dict={}
                temp_dict['user_id']=user_id
                temp_dict['time_stamp']=time_stamp
                prev_user_id=user_id
                prev_time_stamp=time_stamp
                
            index=obj.key.split('/')[2].split('.')[0]
            key=obj.key
            url = generate_presigned_url(s3_client, client_action, {'Bucket': bucket_pre, 'Key': key}, 604800)
            temp_dict[index]=url
        
        #temp_dict['renew_time']=str(int(datetime_kst.timestamp()))
        temp_list.append(temp_dict)
        print(len(temp_list))
        temp_list.pop(0)
        dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
        table=dynamodb.Table(f"Dy_story_presigned_url_prod")
        for item in temp_list:
            temp=table.put_item(
                Item=item
            )
    except:
        print("why?")
    '''
    # 캐릭터 renew
    try:
        s3_client=boto3.client('s3')
        bucket_pre="s3-kkobook-character"
        client_action="get_object"
        
        dynamodb_client=boto3.client('dynamodb')
        query=f"select * from Dy_user_character_prod"
        result=dynamodb_client.execute_statement(Statement=query)
        
        for item in result["Items"]:
            try:
                # presigned url 재발급이 필요한 친구들
                user_id=item['user_id']['S']
                time_stamp =item['time_stamp']['S']
                #print(user_id,time_stamp)
                key=item['object_key']['S']
                url = generate_presigned_url(s3_client, client_action, {'Bucket': bucket_pre, 'Key': key}, 604800)
                #print(url)
                query=f"UPDATE Dy_user_character_prod SET img_url = '{url}' WHERE user_id='{user_id}' and time_stamp='{time_stamp}'"
                result_update=dynamodb_client.execute_statement(Statement=query)
                #print("good")
            except:
                pass
    except:
        print("why!!!?")
        
        
    # 동화 새로운 버전 presigned url 갱신
    try:

        s3_client=boto3.client('s3')
        bucket_pre="s3-kkobook-story-image"
        client_action="get_object"
        
        
        dynamodb_client=boto3.client('dynamodb')
        query=f"select user_id from Dy_story_image_prod"
        result=dynamodb_client.execute_statement(Statement=query)
        
        
        dynamodb_client=boto3.client('dynamodb')
        query=f"select * from Dy_user_book_prod"
        result_user_book=dynamodb_client.execute_statement(Statement=query)
        
        dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
        table=dynamodb.Table(f"Dy_story_image_prod")
        
        cnt=0
        for item in result_user_book["Items"]:
            temp={}
            user_id=item["user_id"]["S"]
            time_stamp=item["time_stamp"]["S"]
            temp['user_id']=user_id
            temp['time_stamp']=time_stamp
            
            try:
                query=f"select page_index from Dy_story_image_prod where user_id='{user_id}' and time_stamp='{time_stamp}'"
                result=dynamodb_client.execute_statement(Statement=query)
                page_index=result["Items"][0]["page_index"]['S']
                #print(page_index)
                cnt+=1
            except:
                print("???")
            
            
            temp['page_index']=page_index
            for page in range(1,9):
                key=f"upscale/{user_id}/{time_stamp}/{page}/{page_index[page-1]}.jpg"
                url = generate_presigned_url(s3_client, client_action, {'Bucket': bucket_pre, 'Key': key}, 604800)
                temp[str(page)]=url
                

            tp=table.put_item(
                Item=temp
            )

        print(cnt)
    except:
        print("??")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
