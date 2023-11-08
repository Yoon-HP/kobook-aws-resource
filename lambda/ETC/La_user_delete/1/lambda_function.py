import json
import boto3
import urllib.request


def post_slack(argStr):
    message = argStr
    send_data = {
        "text": message,
    }
    send_text = json.dumps(send_data)
    request = urllib.request.Request(
        "slack webhook url",
        data=send_text.encode("utf-8"),
    )

    with urllib.request.urlopen(request) as response:
        slack_message = response.read()


# 삭제 요청을 sqs에 전달하고 최종 처리는 향후 진행
dynamodb_client = boto3.client("dynamodb")


def lambda_handler(event, context):
    # test code 확인
    print(event)
    # version check
    function_arn = context.invoked_function_arn
    env = function_arn.split(":")[-1]
    print(env)

    # event parsing (from sqs)
    try:
        message_body = json.loads(event["Records"][0]["body"])
        user_id = message_body["user_id"]
    except:
        print("event_parsing_fail!!!!!")
        post_slack(f"from La_user_delete user delete fail!!! user_id: {user_id}")

    # Dy_character_event (user_id, time_stamp)
    try:
        # get image!
        query = f"select * from Dy_character_event_{env} where user_id='{user_id}'"
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time_stamp = item["time_stamp"]["S"]
            query = f"delete from Dy_character_event_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # Dy_gpt_prompt (user_id, time_stamp)
    try:
        # get image!
        query = f"select * from Dy_gpt_prompt_{env} where user_id='{user_id}'"
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time_stamp = item["time_stamp"]["S"]
            query = f"delete from Dy_gpt_prompt_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # Dy_gpt_story (user_id, time_stamp)
    try:
        # get image!
        query = f"select * from Dy_gpt_story_{env} where user_id='{user_id}'"
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time_stamp = item["time_stamp"]["S"]
            query = f"delete from Dy_gpt_story_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # Dy_midjourney_output_character (user_id, time)
    try:
        # get image!
        query = f"select * from Dy_midjourney_output_character_{env} where user_id='{user_id}'"
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time = item["time"]["S"]
            query = f"delete from Dy_midjourney_output_character_{env} where user_id='{user_id}' and \"time\"='{time}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # Dy_midjourney_output_character_upscale (user_id, time)
    try:
        # get image!
        query = f"select * from Dy_midjourney_output_character_upscale_{env} where user_id='{user_id}'"
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time = item["time"]["S"]
            query = f"delete from Dy_midjourney_output_character_upscale_{env} where user_id='{user_id}' and \"time\"='{time}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # Dy_midjourney_output_story (user_id, time)
    try:
        # get image!
        query = (
            f"select * from Dy_midjourney_output_story_{env} where user_id='{user_id}'"
        )
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time = item["time"]["S"]
            query = f"delete from Dy_midjourney_output_story_{env} where user_id='{user_id}' and \"time\"='{time}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # Dy_midjourney_output_story_upscale (user_id, time)
    try:
        # get image!
        query = f"select * from Dy_midjourney_output_story_upscale_{env} where user_id='{user_id}'"
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time = item["time"]["S"]
            query = f"delete from Dy_midjourney_output_story_upscale_{env} where user_id='{user_id}' and \"time\"='{time}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # Dy_story_event (user_id, time_stamp)
    try:
        # get image!
        query = f"select * from Dy_story_event_{env} where user_id='{user_id}'"
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time_stamp = item["time_stamp"]["S"]
            query = f"delete from Dy_story_event_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # Dy_user_book (user_id, time_stamp)

    try:
        # get image!
        query = f"select * from Dy_user_book_{env} where user_id='{user_id}'"
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time_stamp = item["time_stamp"]["S"]
            query = f"delete from Dy_user_book_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # Dy_user_character (user_id, time_stamp)
    try:
        # get image!
        query = f"select * from Dy_user_character_{env} where user_id='{user_id}'"
        result = dynamodb_client.execute_statement(Statement=query)

        for item in result["Items"]:
            time_stamp = item["time_stamp"]["S"]
            query = f"delete from Dy_user_character_{env} where user_id='{user_id}' and time_stamp='{time_stamp}'"
            temp = dynamodb_client.execute_statement(Statement=query)
    except:
        print("delete fail!!")
        post_slack(f"user delete fail!!! user_id: {user_id}")

    # s3 delete
    s3 = boto3.resource("s3")
    # s3-kkobook-character
    bucket = s3.Bucket("s3-kkobook-character")

    # cut
    prefix = f"cut/{user_id}"
    for obj in bucket.objects.filter(Prefix=prefix):
        print(obj.key)
        s3.Object(bucket.name, obj.key).delete()

    # composite
    prefix = f"composite/{user_id}"
    for obj in bucket.objects.filter(Prefix=prefix):
        print(obj.key)
        s3.Object(bucket.name, obj.key).delete()

    # output
    prefix = f"output/{user_id}"
    for obj in bucket.objects.filter(Prefix=prefix):
        print(obj.key)
        s3.Object(bucket.name, obj.key).delete()

    # s3-kkobook-book
    bucket = s3.Bucket("s3-kkobook-book")
    prefix = f"{user_id}"
    for obj in bucket.objects.filter(Prefix=prefix):
        print(obj.key)
        s3.Object(bucket.name, obj.key).delete()

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
