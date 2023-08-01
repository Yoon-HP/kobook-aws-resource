import json
import sys

sys.path.append("/mnt/efs/packages37")
import boto3
import os
import cv2
import mediapipe as mp
import numpy
import base64
from google.protobuf.json_format import MessageToDict
from botocore.exceptions import ClientError
from PIL import Image, ImageOps
from io import BytesIO


def lambda_handler(event, context):
    # TODO implement

    # dynamodb = boto3.resource('dynamodb',region_name='ap-northeast-2')
    # table=dynamodb.Table("Dy_character_event")

    # dynamodb_client=boto3.client('dynamodb')

    statusCode = 200
    try:
        httpMethod = event["httpMethod"]
        user_id = event["requestContext"]["authorizer"]["claims"]["cognito:username"]
    except:
        print("firebase user!!")

    try:
        httpMethod = event["routeKey"].split()[0]
        user_id = event["requestContext"]["authorizer"]["jwt"]["claims"]["user_id"]
    except:
        print("cognito user!!")
    """
    if httpMethod!="POST":
        statusCode=405
        return {
            'statusCode': statusCode,
            'body': json.dumps('Method Not Allowed')
        }    
    """
    try:
        # firebaes body 관련 추가 작업
        body = json.loads(event["body"])
        print("check")
        img_bin = body["image"]
        img_bin = base64.b64decode(img_bin)
        temp = BytesIO(img_bin)
        img = Image.open(temp)
        print(img.size)
        while img.height > 1024 or img.width > 1024:
            img = img.resize((int(img.width / 2), int(img.height / 2)))

        # 회전 원래대로 되돌리기
        img = ImageOps.exif_transpose(img)

        img.save("/tmp/temp.png")

        s3 = boto3.client("s3")
        # s3.upload_file("/tmp/temp.png","s3-kkobook-character", f"why.png")

        # media
        mp_face_detection = mp.solutions.face_detection
        face_detection = mp_face_detection.FaceDetection(
            model_selection=1, min_detection_confidence=0.3
        )
        img = cv2.imread("/tmp/temp.png")
        results = face_detection.process(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
        # 개수확인가능
        try:
            check = len(results.detections)
            if check >= 2:
                statusCode = 500
                return {
                    "statusCode": statusCode,
                    "body": json.dumps("Internet Server Error"),
                }
        except:
            print("얼굴 인식 실패!!")
            statusCode = 400
            return {"statusCode": statusCode, "body": json.dumps("Bad Request")}
        bbox = results.detections[0].location_data.relative_bounding_box
        h, w, _ = img.shape
        # face =  img[int(h*bbox.ymin):int(h*bbox.ymin)+int(h*bbox.height), int(w*bbox.xmin):int(w*bbox.xmin)+int(w*bbox.width)]
        face = img[
            int(h * (bbox.ymin) * 0.8) : int(h * bbox.ymin)
            + int(h * (bbox.height) * 1.05),
            int(w * (bbox.xmin) * 0.95) : int(w * bbox.xmin) + int(w * bbox.width),
        ]
        cv2.imwrite("/tmp/result.png", face)

        # cut 사진 upload
        s3.upload_file("/tmp/result.png", "s3-kkobook-character", f"cut/{user_id}.png")

    except:
        print("image processing fail!!!")
        return {"statusCode": 400, "body": json.dumps("bad!")}

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
