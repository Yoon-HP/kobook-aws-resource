import json
import sys
sys.path.append("/mnt/efs/packages37")
import boto3
import os
import cv2
import mediapipe as mp
import numpy
import base64
import time
from datetime import datetime, timedelta, timezone
from google.protobuf.json_format import MessageToDict
from botocore.exceptions import ClientError
from PIL import Image, ImageOps
from io import BytesIO

def lambda_handler(event, context):
    # TODO implement

    #dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
    #table=dynamodb.Table("Dy_character_event")
    
    #dynamodb_client=boto3.client('dynamodb')

    # test code 확인
    print(event)
    
    # ping check
    try:
        if 'ping' in event:
            print("ping!!")
            return {
                'statusCode': 200,
                'body': json.dumps('Hello from Lambda!')
            }
    except:
        print("keep going")
        
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]
    
    statusCode=200
    try:
        httpMethod=event['httpMethod']
        user_id=event['requestContext']['authorizer']['claims']['cognito:username']
    except:
        print("firebase user!!")
        
    try:
        httpMethod=event['routeKey'].split()[0]
        user_id=event['requestContext']['authorizer']['jwt']['claims']['user_id']
    except:
        print("cognito user!!")
    '''
    if httpMethod!="POST":
        statusCode=405
        return {
            'statusCode': statusCode,
            'body': json.dumps('Method Not Allowed')
        }    
    '''
    try:
        # firebaes body 관련 추가 작업 
        body=json.loads(event['body'])
        print("check")
        img_bin=body['image']
        img_bin=base64.b64decode(img_bin)
        temp=BytesIO(img_bin)
        img=Image.open(temp)
        
        
        while (img.height>2048 or img.width>2048):
            img=img.resize((int(img.width/2),int(img.height/2)))
        
        # 회전 원래대로 되돌리기
        img=ImageOps.exif_transpose(img)
        
        print(img.size)
        
        img.save('/tmp/temp.png')
        
        s3 = boto3.client('s3')
        # s3.upload_file("/tmp/temp.png","s3-kkobook-character", f"why.png")


        # media
        mp_face_detection = mp.solutions.face_detection
        face_detection = mp_face_detection.FaceDetection(model_selection=1, min_detection_confidence=0.3)
        img = cv2.imread("/tmp/temp.png")
        results = face_detection.process(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
        # 개수확인가능
        try:
            check=len(results.detections)
            print(check)
            if check>=2:
                statusCode=500
                print("image fail!")
                return {
                    'statusCode': statusCode,
                    'body': json.dumps('Internet Server Error')
                }
        except:
            print("얼굴 인식 실패!!")
            statusCode=400
            return {
                'statusCode': statusCode,
                'body': json.dumps('Bad Request')
            }
        bbox = results.detections[0].location_data.relative_bounding_box 
        h, w, _ = img.shape
        #face =  img[int(h*bbox.ymin):int(h*bbox.ymin)+int(h*bbox.height), int(w*bbox.xmin):int(w*bbox.xmin)+int(w*bbox.width)]
        face =  img[int(h*(bbox.ymin)*0.8):int(h*bbox.ymin)+int(h*(bbox.height)*1.05), int(w*(bbox.xmin)*0.95):int(w*bbox.xmin)+int(w*bbox.width)]    
        cv2.imwrite('/tmp/result.png', face)
        


        try:
            o_w,o_h=face.shape[1],face.shape[0]
            new_dimensions=(int(o_w)*2, int(o_h)*2)
            resized_face=cv2.resize(face,new_dimensions,interpolation=cv2.INTER_LANCZOS4)
            print("hi")
            cv2.imwrite('/tmp/result.png', resized_face)
        except:
            print("???")

        
        # cut 사진 upload
        s3.upload_file("/tmp/result.png","s3-kkobook-character", f"cut/{user_id}.png")

        datetime_utc = datetime.utcnow()
        timezone_kst = timezone(timedelta(hours=9))
        # 현재 한국 시간
        datetime_kst = datetime_utc.astimezone(timezone_kst)
        time_order=str(int(datetime_kst.timestamp()))
        s3.upload_file("/tmp/result.png","s3-kkobook-character", f"cut/{user_id}/{time_order}.png")
        time.sleep(0.5)
    except:
        print("image processing fail!!!")
        return {
            'statusCode': 400,
            'body': json.dumps('bad!')
        }
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
