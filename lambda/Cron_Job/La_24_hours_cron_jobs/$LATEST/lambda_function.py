import json
import boto3
import time

def lambda_handler(event, context):
    # TODO implement
    
    
    dynamodb_client = boto3.client("dynamodb")
    dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-2")
    
    query = f"select * from Dy_user_book_prod"
    result = dynamodb_client.execute_statement(Statement=query)
    print(len(result["Items"]))
    
    table_user_coin=dynamodb.Table(f"Dy_user_prod")
    
    for item in result["Items"]:
        coin_temp={}
        user_id = item["user_id"]["S"]
        #time_stamp = item["time_stamp"]["S"]
        
        coin_temp['user_id']=user_id
        #coin_temp['time_stamp']=time_stamp
        coin_temp['coin']='1'
        
        temp = table_user_coin.put_item(Item=coin_temp)
        
    query = f"select * from Dy_user_character_prod"
    result = dynamodb_client.execute_statement(Statement=query)
    print(len(result["Items"]))
    
    table_user_coin=dynamodb.Table(f"Dy_user_prod")
    
    for item in result["Items"]:
        coin_temp={}
        user_id = item["user_id"]["S"]
        #time_stamp = item["time_stamp"]["S"]
        
        coin_temp['user_id']=user_id
        #coin_temp['time_stamp']=time_stamp
        coin_temp['coin']='1'
        
        temp = table_user_coin.put_item(Item=coin_temp)
        
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
