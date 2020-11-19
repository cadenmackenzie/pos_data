import os
import time
import requests
import json
import boto3
from urllib.parse import unquote_plus
import pandas as pd
import numpy as np

print('Loading Function')
s3_client = boto3.client('s3')

def load_data(d, server='development'):
    f = requests.post(f"https://{server}.handofftech.com/v2/util/addUpdateInventoryExtension?apiKey={os.environ['api_key']}",
        headers={"Content-Type": "application/json"}, 
        json=d)
    return f

def batch_load(iterable, batch_size=1):
    l = len(iterable)
    for ndx in range(0,l,batch_size):
        yield iterable[ndx:min(ndx + batch_size, l)].to_dict(orient='records')

def transform_nans(df):
    for c in df.columns:
        df[c] = df[c].replace(np.nan, '', regex=True)
    return df
    
def load_pos_to_db(filename):
    df = pd.read_csv(filename)
    df = transform_nans(df)

    # Load row-by-row
    for i, d in enumerate(batch_load(df)):
        if i % 100 == 0:
            print(f'Loaded row: {i}')

        f = load_data(d[0])
        if i == 0:
            print(f)

        if f.status_code != 200:
            raise Exception(f'Post request return {f.status_code}. Here is the problem row: {d[0]}')
    return

def lambda_handler(event, context):
    start = time.time()

    for record in event['Records']:
        # Configure variables to get file from S3 bucket
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}'.format(tmpkey)

        print(f'{download_path} has been loaded to {bucket}.')
        
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
