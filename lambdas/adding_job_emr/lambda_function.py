import json
import boto3
import time 
from uuid import uuid4

def lambda_handler(event, context):
    ###Check that we are adding new job not removing one
    print(event['Records'][0]['eventName'])
    if event['Records'][0]['eventName'] == 'REMOVE':
        print("delete action")
        ##END PROCESS
    else :
        input_folder = event['Records'][0]['dynamodb']['Keys']['jobs']['S']
        
        # print(input_folder)
        input_folder_split = input_folder.split('/') #remove file part
        # print(input_folder_split[0])
        input_folder_name = 's3://courseworkmapreduce/' + input_folder_split[0] + "/" + input_folder_split[1] + "/"
        print(input_folder_name)
        
        #format a input string depending on input_
        
        output_folder_name = "s3://courseworkmapreduce/output/output/" + input_folder_split[0] + "/" + input_folder_split[1] + "/" + str(uuid4) + "/"
        
        print("is this a new folder name")
        print(output_folder_name)
        
        connection = boto3.client(
            'emr',
            region_name='us-east-1'
        )
        
        response = connection.list_clusters(ClusterStates=['WAITING','RUNNING','STARTING'])
        
        #FAIL when there's no clusters
        if len(response.get('Clusters')) == 0:
            ##START A CLUSTER 
            start_clusters(input_folder, input_folder_name, output_folder_name)
            print("New Cluster")

        else :
            clusterId = response.get('Clusters')[0].get('Id')
            print("Adding job to cluster")
            cluster_id = connection.add_job_flow_steps(
                JobFlowId = clusterId,
                Steps=[
                    {
                        'Name' : input_folder,
                        'ActionOnFailure': 'CONTINUE',
                        'HadoopJarStep' : {
                            'Jar' : 'command-runner.jar',
                            'Args' : [
                                'hadoop-streaming',
                                '-files', 's3://courseworkmapreduce/job/mapper.py,s3://courseworkmapreduce/job/reducer.py',
                                '-mapper', 'mapper.py', 
                                '-input', input_folder_name,
                                '-output', output_folder_name,
                                '-reducer', 'reducer.py' #Adding this line means adding it to -files and putting a comma at he end of output
                            ]
                        }
                    }
                ],
            )
        
        # TODO implement
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
    
def start_clusters(input_folder, input_folder_name, output_folder_name):
    
    print("is this a new folder name")
    print(output_folder_name)
    
    connection = boto3.client(
        'emr',
        region_name='us-east-1'
    )
    
    cluster_id = connection.run_job_flow(
        Name="emr",
        LogUri='s3://courseworkmapreduce/logs',
        ReleaseLabel='emr-5.18.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.large',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm4.large',
                    'InstanceCount': 1,
                    'AutoScalingPolicy' : {
                    'Constraints': {
                        'MinCapacity': 1,
                        'MaxCapacity': 4
                    },
                    'Rules': [
                    {
                        'Name': 'emrautoscaleout',
                        'Description': 'scales',
                        'Action': {
                            'SimpleScalingPolicyConfiguration': {
                                'AdjustmentType': 'CHANGE_IN_CAPACITY',
                                'ScalingAdjustment': 1,
                            }
                        },
                        'Trigger': {
                            'CloudWatchAlarmDefinition': {
                                'ComparisonOperator': 'GREATER_THAN_OR_EQUAL',
                                'MetricName': 'TotalLoad',
                                'Period': 300,
                                #over 20% CPU and then we spin up another 
                                'Threshold': 2,
                                'CoolDown': 600
                            }
                        }
                    },
                    {
                    'Name': 'emrautoscalein',
                    'Description': 'scales',
                    'Action': {
                        'SimpleScalingPolicyConfiguration': {
                        'AdjustmentType': 'CHANGE_IN_CAPACITY',
                        'ScalingAdjustment': -1,
                        }
                    },
                    'Trigger': {
                        'CloudWatchAlarmDefinition': {
                        'ComparisonOperator': 'GREATER_THAN',
                        'MetricName': 'IsIdle',
                        'Period': 300,
                        #over 20% CPU and then we spin up another 
                        'Threshold': 1,
                        }
                    }
                },
                ]
                }
                },
            ],
            'Ec2KeyName': 'cloud-key',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
        },
        Steps=[
            {
                'Name' : input_folder,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep' : {
                    'Jar' : 'command-runner.jar',
                    'Args' : [
                        'hadoop-streaming',
                        '-files', 's3://courseworkmapreduce/job/mapper.py,s3://courseworkmapreduce/job/reducer.py',
                        '-mapper', 'mapper.py', 
                        '-input', input_folder_name,
                        '-output', output_folder_name,
                        '-reducer', 'reducer.py'
                    ]
                }
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        AutoScalingRole="EMR_AutoScaling_DefaultRole"
    )
    return 0
    
    
