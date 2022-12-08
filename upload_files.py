#!/usr/bin/python
import logging
import boto3
from botocore.exceptions import ClientError
import os
import sys
## START OF CHAIN

def upload_file(file_name, bucket, job_number, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    input_string = 'input/input' + str(job_number) + '/' + object_name
    input_string = input_string.replace(" ", "")
    print('Uploaded: input/input' , job_number , '/', object_name)
    #print(bucket.name)
    try:
        response = s3_client.upload_file('./books/' + file_name, bucket.name, input_string)
    except ClientError as e:
        logging.error(e)
        return False
    return True

directory = os.fsencode('./books')
s3 = boto3.resource('s3')
s3_bucket = s3.Bucket('courseworkmapreduce')
s3_client = boto3.client('s3')
        

for job_number in range(0,50):
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".txt"):
            if(upload_file(filename, s3_bucket, job_number) == False):
                    print("FAIL")
                    sys.exit(0)
            continue
        else:
            continue


# for file in os.listdir(directory):
#     filename = os.fsdecode(file)
#     if filename.endswith(".txt"): 
#         if(upload_file(filename, s3_bucket, job_number) == False):
#             print("FAIL")
#             sys.exit(0)
#         # print(os.path.join(directory, filename))
#         continue
#     else:
#         continue