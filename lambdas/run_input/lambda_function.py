import json
import boto3

def lambda_handler(event, context):
    print("event: ", event)
    folder_job = event['Records'][0]['s3']['object']['key']
    #print(event.get['Records'].get['s3'])
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('text_job')
    table.put_item(Item={'jobs' : folder_job})
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
