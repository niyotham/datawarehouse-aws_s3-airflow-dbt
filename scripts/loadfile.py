import sys, os
from dotenv import dotenv_values, load_dotenv
import boto3

import pandas as pd
sys.path.append("../")

load_dotenv()

##get access variable to aws
# config = dotenv_values(".env")
# region_name= config['region_name']
# aws_access_key_id = config['aws_access_key_id']
# aws_secret_access_key = config['aws_secret_access_key']

region_name= os.getenv('region_name')
aws_access_key_id = os.getenv('aws_access_key_id') 
aws_secret_access_key = os.getenv('aws_secret_access_key')


def create_s3_client(region_name,aws_access_key_id,aws_secret_access_key):
    """ Function to get aws s3 client"""
    s3_client = boto3.client('s3',
    region_name= region_name,
    aws_access_key_id = aws_access_key_id ,
    aws_secret_access_key = aws_secret_access_key )
    return s3_client

def create_sns_client(region_name,aws_access_key_id,aws_secret_access_key):
    """ Function to get aws sns client"""
    sns_client = boto3.client('sns',
    region_name= region_name,
    aws_access_key_id = aws_access_key_id ,
    aws_secret_access_key = aws_secret_access_key )
    return sns_client

def upload_file_to_s3(client, bucket_name, filename, key):
    try:
        client.upload_file(Bucket=bucket_name,
                # Set filename and key
                Filename=filename, 
                Key=key,
                #  ExtraArgs = {
                #     'ACL': 'public-read'}
                        )
        print(f'file uploaded successfull to {bucket_name} ')
    except Exception as e:
        print(f'File not uploaded because of the {e} error.')


#  create a bucket
if __name__ == "__main__":
    s3_client = create_s3_client(region_name,aws_access_key_id,aws_secret_access_key)
    sns_client=create_sns_client(region_name,aws_access_key_id,aws_secret_access_key)
    response_staging= s3_client.create_bucket(Bucket='tham-staging')
    response_processed = s3_client.create_bucket(Bucket='tham-processed')
    response = s3_client.list_buckets()['Buckets']
    # for bucket in response:
    #     print(bucket['Name'])
    # print(res['Contents'][1]['Key'])
    # print(sns_client.list_topics())
    bucket_name ='tham-staging'
    filenames = ['../data/vehicles_data.csv', '../data/trajectories_data.csv']
    for filename in filenames:
        # form a key pattem
        name = filename.split('/')[-1].split(".")[0]
        key = f'2024/{name}_01_01.csv'
        upload_file_to_s3(s3_client, bucket_name, filename,key)
    
        # Get object metadata and print it
        response = s3_client.head_object(Bucket='tham-staging', 
                            Key=key)

        # Print the size of the uploaded object
        print(response['ContentLength'])
    # List only objects that start with '2018/final_'
    response = s3_client.list_objects(Bucket='tham-staging', 
                        Prefix='2024/')
    for obj in response['Contents']:
        print(obj["Key"])
