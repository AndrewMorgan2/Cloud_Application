import json
import boto3

def lambda_handler(event, context):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('text_job')
    job_name = event['detail']['name']
    
    ##Destory item 
    response = table.delete_item(Key={'jobs' : job_name})
    print(response)
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
