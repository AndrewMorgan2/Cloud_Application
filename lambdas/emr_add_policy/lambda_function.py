import json
import boto3

def lambda_handler(event, context):
    ###NOW PART OF CREATE EMR 
    
    connection = boto3.client(
        'emr',
        region_name='us-east-1'
    )
    response = connection.list_clusters(ClusterStates=['STARTING'])
    clusterId = response.get('Clusters')[0].get('Id')
    
    response = connection.list_instance_groups(
        ClusterId= clusterId,
    )
    workerId = response.get('InstanceGroups')[0].get('Id')
    if(response.get('InstanceGroups')[0].get('Name') != 'Slave nodes'):
        print("Uh oh, we aren't looking at the slaves")
    
    # response = connection.list_instance_groups(
    #     ClusterId='j-2TRKTLSJS0L94'
    # )
    # print(len(response['InstanceGroups']))
    
    ##WORKS BUT I HAVEN'T ACTUALLY SEEN SCALING YET
    ##VARS BELOW DEFINE THE SCALING POLICY
    response = connection.put_auto_scaling_policy(
        ClusterId= clusterId,
        InstanceGroupId= workerId,
        AutoScalingPolicy={
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
    )
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
