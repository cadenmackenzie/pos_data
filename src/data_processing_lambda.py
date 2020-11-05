import time
import requests
import json
import boto3
import uuid
from urllib.parse import unquote_plus
import pandas as pd
# import pysal.lib.io as ps

print('Loading Function')

s3_client = boto3.client('s3')

class processMPower(object):
    def __init__(self):
        self.cols = [
            'rt_product_id','rt_upc_code','rt_brand_name',
            'rt_brand_description','rt_product_type','rt_product_category',
            'rt_package_size','rt_item_size','price_regular',
            'price_sale','qty_on_hand'
            ]
        self.col_names_dict = {
            'product_id':'rt_product_id',
            'upc_code':'rt_upc_code',
            'brand_name':'rt_brand_name',
            'brand_description':'rt_brand_description',
            'product_type':'rt_product_type',
            'product_category':'rt_product_category',
            'package_size':'rt_package_size',
        }
        pass
    
    def load_data(self, input_filenames):
        # Check if input input_filenames is list
        if not isinstance(input_filenames, list):
            raise Exception("input_filenames is not a list - expecting input_filenames to be a list with single .csv file.")
        
        # Check if input input_filenames is list  
        if len(input_filenames) > 1:
            raise Exception("More than one file passed - expecting a single .csv file in input_filenames.")
        
        if '.csv' in input_filenames[0]:
            df = pd.read_csv(input_filenames[0]) # read in filename as str
            return df
        else:
            raise Exception("Unrecognized file type - expecting .csv extension.")
        pass

    def process_data(self, df):
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)
        # Check if sale_price in the dataframe if not then set all sale_price to 0
        if 'sale_price' not in df.columns: 
            df['sale_price'] = 0
        
        # Check if item_size in the dataframe if not then set all item_size to empty string 
        if 'rt_item_size' not in df.columns:
            df['item_size'] = ''
        
        df = df[self.cols]
        return df

# class processPOSNAME2Pos(processPOSNAME1Pos):
#     def __init__(self):
#         self.col_names_dict = {
#             'code_num':'rt_product_id',
#             'barcode':'rt_upc_code',
#             'brand':'rt_brand_name',
#             'descrip':'rt_brand_description',
#             'typenam':'rt_product_type',
#             'typenam':'rt_product_category',
#             'size':'rt_package_size',
#             'price':'price_regular',
#             'qty_on_hnd':'qty_on_hand'
#         }
#         pass

#     def read_dbf_files(self, input_filename): #Reads in DBF files and returns Pandas DF
#         '''
#         Arguments
#         ---------
#         dbfile  : DBF file - Input to be imported
#         adapted from: https://www.kaggle.com/jihyeseo/dbf-file-into-pandas-dataframe
#         '''
#         db = ps.open(input_filename) #Pysal to open DBF
#         d = {col: db.by_col(col) for col in db.header} #Convert dbf to dictionary
#         df = pd.DataFrame(d) #Convert to Pandas DF
#         db.close() 
#         return df

#     def load_data(self, input_filenames):
#         count = 0
#         for f in input_filenames:
#             if '.dbf' in f:
#                 df = self.read_dbf_files(f)
#                 if f == 'BARCODES.dbf':
#                     df = df[['CODE_NUM','BARCODE']]
#                 elif f == 'LIQCODE.dbf':
#                     df.drop(['BARCODE'], axis=1, inplace=True)
                    
#                 if count == 0:
#                     final_df = df
#                 else:
#                     if 'CODE_NUM' in final_df.columns and 'CODE_NUM' in df.columns:
#                         final_df = final_df.merge(df, on=['CODE_NUM'])
#                     else:
#                         raise Exception("Can't find unique key - expecting 'CODE_NUM' to be unique key.")
#                 count += 1
#             else:
#                 raise Exception("Unrecognized file type - expecting .dbf extension.")
#         return final_df

#     def process_data(self, df):
#         df.columns = df.columns.str.lower() # lower case column names
#         df.rename(columns=self.col_names_dict, inplace=True) # rename columns to standard naming for inventoryExtension

#         # Create rt_brand_description as combo of brand and descrip
#         df['rt_brand_description'] = df['rt_brand_name'].astype(str) + " " + df['rt_brand_description'].astype(str)

#         # Check if sale_price in the dataframe if not then set all sale_price to 0
#         if 'sale_price' not in df.columns: 
#             df['sale_price'] = 0
        
#         # Check if item_size in the dataframe if not then set all item_size to empty string 
#         if 'rt_item_size' not in df.columns:
#             df['item_size'] = ''
        
#         df = [self.cols]
#         return df


def get_retailer_info(filename):
    start = time.time()
    r = requests.get("https://development.handofftech.com/v2/util/retailers?apiKey=836804e3-928a-4454-b064-485848cc6336") # TODO: get endpoint from Caden --> pull retailer info and match with prefix of filename
    
    # Read return into pandas dataframe
    retailer_df = pd.DataFrame.from_dict(r.json()['data'])
    end = time.time()
    print('Read from database time: {}(s)'.format(end - start))

    # Use the filename prefix/suffix to retrieve info POS and retailer_id of retailer --> file prefix must be unique
    retailer_id, pos = retailer_df[retailer_df['filename'] == str(filename.split('_')[0]) + '_'][['retailer_id','pos']].iloc[0]
    return retailer_id, pos
    
def process_pos(input_filenames, output_filename):
    retailer_id, retailer_pos = get_retailer_info(input_filenames[0])
    
    if retailer_pos == 'mPower':
        pos_proc = processMPower()
            
    # if retailer_pos == '2':
    #     pos_proc = processPOSNAME2Pos()

    start = time.time()
    df = pos_proc.load_data(input_filenames)
    df = pos_proc.process_data(df) # Processing for specific POS system
    
    # Processing for all POS systems
    df['active'] = 1 # set all items in inventoryExtension file as active
    df['retailer_id'] = retailer_id # set retailer_id as the retailer_id pulled from get_retailer_info()

    # save processed dataframe as a csv
    df.to_csv(output_filename, index=False)

    end = time.time() # stop timer
    print(f'Processing file runtime: {end - start}(s)')
    return

def lambda_handler(event, context):
    start = time.time()

    for record in event['Records']:
        # Configure variables to get file from S3 bucket
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        tmpkey = key.replace('/', '')
        download_path = '/tmp/{}_{}'.format(uuid.uuid4(), tmpkey)
        upload_path = '/tmp/processed-{}'.format(tmpkey)
        
        # Download file from S3 set as the trigger ('handoff-pos-raw')
        s3_client.download_file(bucket, key, download_path)
        
        # Process data using pos_processing function above
        process_pos(download_path, upload_path)
        
        # Upload processed file to different S3 bucket ('handoff-pos-processed')
        s3_client.upload_file(upload_path, bucket.replace('raw','processed'), key)

    end = time.time()
    print(f'Final lambda runtime: {end - start}(s)')
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }



# def lambda_handler(event, context):
#     #print("Received event: " + json.dumps(event, indent=2))

#     # Get the object from the event and show its content type
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
#     try:
#         response = s3_client.get_object(Bucket=bucket, Key=key)
#         print("CONTENT TYPE: " + response['ContentType'])
#         return response['ContentType']
#     except Exception as e:
#         print(e)
#         print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
#         raise e
