import boto3 
import os
import tempfile
import shutil
import pathlib

##ASSUMES LabRole arn exists 
LabRoleArn = 'arn:aws:iam::197767350308:role/LabRole'
#email emailed when steps are failed or completed
email_address = "andrewdmorgan.2@gmail.com"
#lambda names
remove_job = 'removing_job_emr'
add_job = 'adding_job_emr'
input = 'run_input'
recovery = 'emr_recovery'
policy_add = 'emr_add_policy'
#database names
bucket_name = "courseworkmapreduce"
dynamo_name = "text_job"
#names for events
step_success = "emr_step_success"
step_fail = "emr_step_fail"
cluster_checker = "state_check"
cluster_start_up = "emr_policy_add"
#Sns topic 
topic_to_email_admin = "emr_step_monitor"

##Spinning up lamdbas 
def make_lambdas():
    client_lambdas = boto3.client('lambda')
    #Have lists of names and code so we can loop this 
    list_of_lamdba_names = [policy_add, recovery, input, add_job, remove_job]
    list_of_folders_with_lambda_code = ["emr_add_policy", "emr_recovery", "run_input", "adding_job_emr", "removing_job_emr"]
    index = 0
    dir_for_this_file = pathlib.Path().resolve()
    for name in list_of_lamdba_names:
        local_dir_with_script = "lambdas/" + list_of_folders_with_lambda_code[index]
        dir_with_script = os.path.join(dir_for_this_file, local_dir_with_script)
        print(dir_with_script)
        with tempfile.TemporaryDirectory() as temporaryDir:
            path = shutil.make_archive(
                temporaryDir,
                "zip",
                root_dir=dir_with_script
            )

        print("Getting code from ", path)
        code = None
        with open(path, "rb") as file:
            code = file.read()
        try:
            client_lambdas.create_function(
                FunctionName=name,
                Handler= 'lambda_function.lambda_handler',
                Runtime='python3.9',
                Role=LabRoleArn,
                Code=dict(ZipFile = code)
            )
            print("Spun up: ", name)
        except:
            print("FAILED: to setup=", name, " this is commonly because it already exists" )
        index = index + 1

##Make s3 buckets 
def make_s3_bucket():
    client_s3 = boto3.client('s3', region_name='us-east-1')
    try:
        client_s3.create_bucket(
            ACL='public-read',
            Bucket=bucket_name
        )
        print("s3 bucket started", bucket_name)
        ##Now we make the four subfolders 
        subfolders = ['input/', 'ouput/', 'job/', 'logs/']
        for folder in subfolders:
            client_s3.put_object(Bucket=bucket_name, Key=folder)

        print("Added folders")
        ##Upload jobs
        client_s3.upload_file('./mapper.py', bucket_name, 'job/mapper.py')
        client_s3.upload_file('./reducer.py', bucket_name, 'job/reducer.py')
        print("Jobs uploaded ", bucket_name)
    except:
        print("FAILED: to setup=", bucket_name, " this is commonly because it already exists" )

def make_dynamo():
    dynamo_resource = boto3.resource('dynamodb')
    try:
        dynamo_resource.create_table (
        TableName = dynamo_name,
        KeySchema = [
            {
                'AttributeName': 'jobs',
                'KeyType': 'HASH'
            }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName': 'jobs',
                    'AttributeType': 'S'
                }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1
                },
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'KEYS_ONLY'
                }
        )
        print("Started a dynamo db with name: ", dynamo_name)
    except:
        print("FAILED: tryed to setup dynamo name=", dynamo_name)

def make_event_bridge_rules():
    client_event = boto3.client('events')

    try:
        client_event.put_rule(
            Name= cluster_checker,
            EventPattern='{ "source": ["aws.emr"], "detail-type": ["EMR Cluster State Change"], "detail": {"state": ["TERMINATED_WITH_ERRORS"]}}',
            State='ENABLED',
            Description='cluster fail run',
            RoleArn=LabRoleArn,
            EventBusName='default'
        )
        print("Setup", cluster_checker)
    except:
        print("FAILED to setup ", cluster_checker)

    try:
        client_event.put_rule(
            Name= cluster_start_up,
            EventPattern='{"source": ["aws.emr"], "detail-type": ["EMR Cluster State Change"], "detail": {"state": ["STARTING"]}}',
            State='ENABLED',
            Description='add scaling policy to emr',
            RoleArn=LabRoleArn,
            EventBusName='default'
        )
        print("Setup", cluster_start_up)
    except:
        print("FAILED to setup ", cluster_start_up)

    try:
        client_event.put_rule(
            Name=step_success,
            EventPattern='{ "source": ["aws.emr"], "detail-type": ["EMR Step Status Change"], "detail": { "state": ["COMPLETED"]}}',
            State='ENABLED',
            Description='on emr step success triggers remove job from database and sns',
            RoleArn=LabRoleArn,
            EventBusName='default'
        )
        print("Setup", step_success)
    except:
        print("FAILED to setup ", step_success)

    try:
        client_event.put_rule(
            Name=step_fail,
            EventPattern='{ "source": ["aws.emr"], "detail-type": ["EMR Step Status Change"], "detail": { "state": ["FAILED"]}}',
            State='ENABLED',
            Description='on emr step fail triggers sns',
            RoleArn=LabRoleArn,
            EventBusName='default'
        )
        print("Setup", step_fail)
    except:
        print("FAILED to setup ", step_fail)

##Return sns arn
def make_sns_topic():
    ##Doesnt need try as there can be multiple of these
    ##IMPROVEMENT {to-do} check if the admin is already subscribed
    sns = boto3.resource('sns')
    client = boto3.client('sns')
    response = client.create_topic(
        Name= topic_to_email_admin,
    )
    topic = sns.Topic(response['TopicArn'])
    topic.subscribe(
        Protocol='EMAIL',
        Endpoint= email_address,
        ReturnSubscriptionArn=True
    )
    print("Made sns topic", topic_to_email_admin)
    return response['TopicArn']

def make_triggers(sns_arn):
    client_s3 = boto3.client('s3')
    client_lambdas = boto3.client('lambda')
    dynamodb = boto3.resource("dynamodb")
    client_events = boto3.client('events')

    ##ADDING dynamo to add_job
    table = dynamodb.Table(dynamo_name)

    table_arn = table.latest_stream_arn
    response_lambdas = client_lambdas.get_function(
        FunctionName= add_job,
    )
    #Add permission to lambda
    try:
        client_lambdas.add_permission(FunctionName=add_job,
                                StatementId='trigger-a-add-job',
                                Action='lambda:InvokeFunction',
                                Principal='s3.amazonaws.com',
                                SourceArn=table_arn,
                                )
        client_lambdas.get_policy(FunctionName=add_job)

        client_lambdas.create_event_source_mapping(
            EventSourceArn= table_arn,
            FunctionName=add_job,
            Enabled=True,
            StartingPosition='LATEST',
        )
        print("Setup lambda ", add_job, " to get trigger by object creation in the database")
    except:
        print("FAILED: ", add_job, " trigger")

    ##ADDING s3 to runnning input 
    bucket_arn = 'arn:aws:s3:::' + bucket_name

    response_lambdas = client_lambdas.get_function(
        FunctionName= input,
    )
    #Add permission to lambda allowing s3 to notify
    try:
        client_lambdas.add_permission(FunctionName=input,
                                StatementId='trigger-adding-that-job',
                                Action='lambda:InvokeFunction',
                                Principal='s3.amazonaws.com',
                                SourceArn=bucket_arn,
                                )
        client_lambdas.get_policy(FunctionName=input)

        client_s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration= {
                'LambdaFunctionConfigurations':[
                    {
                        'LambdaFunctionArn': response_lambdas['Configuration']['FunctionArn'], 
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {
                                        'Name': 'prefix',
                                        'Value': 'input/'
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        )
        print("Setup lambda ", input, " to get trigger by object creation in", bucket_name)
    except:
        print("FAILED: ", input, " trigger")

    ##ADDING state_check to emr_recovery
    response_lambdas = client_lambdas.get_function(
        FunctionName= recovery,
    )
    try:
        client_events.put_targets(
                Rule= cluster_checker,
                Targets=[{
                    'Id': recovery,
                    'Arn': response_lambdas['Configuration']['FunctionArn'],
                }]
            )

        cluster_checker_response = client_events.describe_rule(
            Name=cluster_checker,
            EventBusName='default'
        )
        client_lambdas.add_permission(
            FunctionName=recovery,
            StatementId='trigger-recovery-emr-event',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=cluster_checker_response['Arn'],
        )
        client_lambdas.get_policy(FunctionName=recovery)

        print("Setup lamdba", recovery, " to get trigger by ", cluster_checker)
    except:
        print("FAILED: ", recovery, " trigger")

    ##ADDING cluster-up to add_policy
    response_lambdas = client_lambdas.get_function(
        FunctionName= policy_add,
    )
    try:
        client_events.put_targets(
                Rule= cluster_start_up,
                Targets=[{
                    'Id': policy_add,
                    'Arn': response_lambdas['Configuration']['FunctionArn'],
                }]
            )

        cluster_start_up_response = client_events.describe_rule(
            Name=cluster_start_up,
            EventBusName='default'
        )
        client_lambdas.add_permission(
            FunctionName=policy_add,
            StatementId='trigger-policy-add-events',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=cluster_start_up_response['Arn'],
        )
        client_lambdas.get_policy(FunctionName=recovery)

        print("Setup lamdba", policy_add, " to get trigger by ", cluster_start_up)
    except:
        print("FAILED: ", policy_add, " trigger")
    
    ##ADDING emr_step_success triggers emr_remove_job
    response_lambdas = client_lambdas.get_function(
        FunctionName= remove_job,
    )
    try:
        client_events.put_targets(
                Rule= step_success,
                Targets=[{
                    'Id': remove_job,
                    'Arn': response_lambdas['Configuration']['FunctionArn'],
                }]
            )

        remove_job_rep = client_events.describe_rule(
            Name=step_success,
            EventBusName='default'
        )
        client_lambdas.add_permission(
            FunctionName=remove_job,
            StatementId='trigger-remove_job_events',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=remove_job_rep['Arn'],
        )
        client_lambdas.get_policy(FunctionName=remove_job)

        print("Setup lamdba", remove_job, " to get trigger by ", step_success)
    except:
        print("FAILED: ", remove_job, " trigger")

    ##ADDING emr_step_fail triggers sns
    client_events.put_targets(
            Rule= step_fail,
            Targets=[{
                'Id': topic_to_email_admin,
                'Arn': sns_arn
            }]
        )
    print("Setup SNS topic (which emails) ", topic_to_email_admin, " to get trigger by ", step_fail)

    ##ADDING emr_step_success triggers sns
    client_events.put_targets(
            Rule= step_success,
            Targets=[{
                'Id': topic_to_email_admin,
                'Arn': sns_arn
            }]
        )
    print("Setup SNS topic (which emails) ", topic_to_email_admin, " to get trigger by ", step_success)

###basically main (where we call functions)
sns_arn = make_sns_topic()
make_dynamo()
make_s3_bucket()
make_event_bridge_rules()
make_lambdas()
make_triggers(sns_arn)

print('Setup finished (hopefully)')
print('Starting to add jobs')
##TO DO