import os
import time
import requests
import json
import boto3
from urllib.parse import unquote_plus

print('Loading Function')
s3_client = boto3.client('s3')

def load_pos_to_db(filename):
    print(f'load_pos_to_db()\n\t-> pushing {filename} to Encompass table ProcessedPosData')
    url = f"https://api.encompass8.com/aspx1/API?APICommand=HandoffLoadProcessedPosData&EncompassID=Handoff&APIToken={os.environ['api_key']}"

    payload={}
    files=[
    ('File',(filename,open(filename,'rb'),'text/csv'))
    ]
    
    headers = {
        'Cookie': 'EncompassDistributor=Handoff; EncompassDBServer=MySQL_Handoff'
    }

    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    print(f'\t-> Response: {response.status_code}')
    
    if response.status_code != 200:
        raise Exception(f'Post request return {response.status_code}.\n{response.text}')
    return response

def lambda_handler(event, context):
    start = time.time()

    for record in event['Records']:
        # Configure variables to get file from S3 bucket
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}'.format(tmpkey)

        print(f'{download_path} has been loaded from {bucket}.')
        
        # Download file from S3 set as the trigger ('handoff-pos-processed')
        s3_client.download_file(bucket, key, download_path)
        
        # Load data using load_pos_to_db function above
        load_pos_to_db(download_path)
        
    print('Function Complete')
    end = time.time()
    print(f'Final lambda runtime: {end - start}(s)')
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }
