import json
import boto3
from presigned_url import generate_presigned_url


def lambda_handler(event, context):
    print(event['rawPath'])
    
    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env=function_arn.split(":")[-1]
    
    # TODO implement
    if len(event['rawPath'])>2:
        object_key=event['rawPath'][1:]

    try:
        s3_client=boto3.client('s3')
        bucket="s3-kkobook-character"
        key=object_key
        print(key)
        client_action="get_object"
        url = generate_presigned_url(
            s3_client, client_action, {'Bucket': bucket, 'Key': key}, 300)

    except:
        statusCode:500
        print("who!!!")
        return {
            'statusCode': statusCode,
            'body': json.dumps('Internel Sever error')
        }

    print("Good!")
    return {
        'statusCode': 302,
        'headers':{'location':url}
    }
