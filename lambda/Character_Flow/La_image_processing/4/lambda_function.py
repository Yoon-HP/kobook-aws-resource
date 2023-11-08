import json
import cv2
import numpy
import boto3
import time
from PIL import Image
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

# dynamodb
dynamodb_client=boto3.client('dynamodb')

def lambda_handler(event, context):

    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]
    
    # event parsing (from sqs)
    try:
        # print(event)
        #message_body=json.loads(event["Records"][0]['body'])
        # bucket_name=message_body['Records'][0]['s3']['bucket']['name']
        # object_key=message_body['Records'][0]['s3']['object']['key']
        # object_key=message_body['key']
        # print(object_key)
        
        message_body=json.loads(event["Records"][0]['body'])
        user_id=message_body['user_id']
        time_stamp=message_body['time_stamp']
        
        query=f"SELECT cloth,h1,w1 FROM Dy_character_event_{env} where user_id='{user_id}' and status='ongoing';"
        result=dynamodb_client.execute_statement(Statement=query)
        cloth=result["Items"][0]['cloth']['S']
        h1=int(result["Items"][0]['h1']['S'])
        w1=int(result["Items"][0]['w1']['S'])
    except:
        print("event_parsing_fail!!!!!")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
        
    try:
        pass
    except:
        pass
            
    # image processing flow
    try:
        s3 = boto3.client('s3')
        # print(object_key)
        # bucket name, object key, download path
        
        # 유저 사진 저장을 안함!
        s3.download_file("s3-kkobook-character",f'cut/{user_id}.png','/tmp/cut_face.png')
        
        # 이름 관련 이슈 존재
        # asset download
        s3.download_file('s3-kkobook-character',f"asset/{cloth}.png",'/tmp/cloth.png')
        
        '''
        img=Image.open("/tmp/temp.png")
        #print(img.size)
        while (img.height>1024 or img.width>1024):
            img=img.resize((int(img.width/2),int(img.height/2)))
        img.save('/tmp/temp.png')
        # image cut process
        img = cv2.imread("/tmp/temp.png")
        # Convert into grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_alt.xml')
        # Detect faces
        faces = face_cascade.detectMultiScale(gray, 1.1, 4)
        #print("face_check :", faces)
        # print(len(faces))
        # face 인식 실패!!!
        print(faces)
        if len(faces)!=1:
            print("face fail!!!")
            query=f"UPDATE Dy_character_event SET fail = 'face_fail' WHERE user_id='{user_id}' and datetime='{datetime}';"
            result=dynamodb_client.execute_statement(Statement=query)
            statusCode=500
            return {
                'statusCode': statusCode,
                'body': json.dumps('face_fail!!!')
            }
        
        # Draw rectangle around the faces and crop the faces
        for (x, y, w, h) in faces:
            # cv2.rectangle(img, (x, y), (x+w, y+h), (0, 0, 255), 2)
            faces = img[y:y + int(h*1.2), x:x + int(w*1.2)]
            # custom naming 필요
            cv2.imwrite('/tmp/temp_face.png', faces)
        '''
        
        # make new image (face + asset)
        im_face=Image.open('/tmp/cut_face.png')
        im_asset=Image.open('/tmp/cloth.png')
        
        # resize (1024,1024)
        im_asset=im_asset.resize((1024,1024))
        
        new_im=Image.new("RGB",(1500,1500),(255,255,255))
        
        w0=im_asset.width ; h0=im_asset.height
        w=new_im.width ; h=new_im.height
        w2=im_face.width ; h2=im_face.height
        if h2>500:
            im_face=im_face.resize((400,400))
        if w2<w1:
            im_face=im_face.resize((w1,max(w1,h2)))
        
        # test
        im_face=im_face.resize((w1*2,w1*2))
        
        w2=im_face.width ; h2=im_face.height
    
        new_im.paste(im_asset,(int((w-w0)/2),h-h0))
        new_im.paste(im_face,(int((w-w2)/2),h-(h1+h2)))
        
        new_im=new_im.resize((1024,1024))
        new_im.save("/tmp/result.png")

        s3.upload_file("/tmp/result.png","s3-kkobook-character", f"composite/{user_id}/{time_stamp}.png")
        
    except:
        # 유저가 잘못된 사진을 올렸을 경우도 파악 가능
        print("image processing flow fail!!!!!")
        query=f"UPDATE Dy_character_event_{env} SET fail = 'image_fail' WHERE user_id='{user_id}' and time_stamp='{time_stamp}';"
        result=dynamodb_client.execute_statement(Statement=query)
    
    
    
    # character 생성 요청 기록
    try:
        key=f"composite/{user_id}/{time_stamp}.png"
        # dynamodb
        dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
        table=dynamodb.Table(f"Dy_midjourney_check_{env}")
        message_body_dy={}
        message_body_dy['pk']=key
        message_body_dy['check']="start"
        # dynamodb put!
        temp=table.put_item(
            Item=message_body_dy
        )
    except:
        print("dynamodb put fail!!")
    
    # SQS에 전달하기 전 현재 discord 상태 파악
    try:
        query=f"SELECT * from Dy_midjourney_check_{env} where \"check\"='yes' or \"check\"='no'";
        result=dynamodb_client.execute_statement(Statement=query)
        print(result)
        
        # 현재 디스코드 채널 상에서 작업중인 job의 개수
        job_number=len(result["Items"])
        print(job_number)
    except:
        print("dynamodb query fail!!")
    
    
    # 현재 디스코드 채널 상의 job 개수를 고려해 sqs에 전달
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
        for i in range(min(len(time_sort),max(10-job_number,0))):
            
            '''
            # SQS에 직접 전송
            try:
                # 최종 처리는 sqs에 연결된 lambda가 진행
                sqs = boto3.resource('sqs', region_name='ap-northeast-2')
                queue = sqs.get_queue_by_name(QueueName=f"SQS_post_midjourney_character_{env}")
                
                # key에 pk 넣기
                key=time_sort[i][1]
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
            except ClientError as error:
                logger.exception("Send Upscale message failed: %s", message_body_sqs)
                raise error
            '''
            # midjourney post lambda invoke!
            try:
                lambda_client=boto3.client('lambda')
                
                payload={
                    'key':time_sort[i][1]
                }
                response = lambda_client.invoke(
                    FunctionName='La_post_midjourney_character:prod',
                    InvocationType='Event',
                    Payload=json.dumps(payload)
                )
            except:
                print("lambda invoke fail!")
            time.sleep(1)
    except:
        print("something went wrong!!")
    
    

    
    print("Good!")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
