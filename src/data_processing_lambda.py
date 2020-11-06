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
        # Check if price_sale in the dataframe if not then set all price_sale to 0
        if 'price_sale' not in df.columns: 
            df['price_sale'] = 0
        
        # Check if item_size in the dataframe if not then set all rt_item_size to empty string 
        if 'rt_item_size' not in df.columns:
            df['rt_item_size'] = ''
        
        df = df[self.cols]
        return df


class processTiger(processMPower):
    def __init__(self):
        self.col_names_dict = {
            'itemid':'rt_product_id',
            'itemscanid':'rt_upc_code',
            'itemorder':'rt_brand_name',
            'itemname':'rt_brand_description',
            'deptid':'rt_product_type',
            # 'deptid':'rt_product_category',
            'isize':'rt_package_size',
            'stdprice':'price_regular',
            'webprice':'price_sale',
            'qtyonhand':'qty_on_hand'
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
            df = pd.read_csv(input_filenames[0], sep='|', encoding='ISO-8859-1') # read in filename as str using | as delimiter
            return df
        else:
            raise Exception("Unrecognized file type - expecting .csv extension.")
        pass

    def process_data(self, df):
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        # Drop row where no product_id is provided (maybe not the case where it has to be a digit)
        # if product_id can not be a digit then change this to simply drop the first row
        df = df[df['rt_product_id'].apply(lambda x: str(x).isdigit())]

        # For both rt_product_type and rt_product_category turn deptid into category
        df['rt_product_type'] = df['rt_product_type'].apply(lambda x: 'BEER' if x == 3 else 'LIQUOR' if x == 2 else 'WINE' if x == 4 else 'EXTRAS')

        # set rt_product_category to equal rt_product_type since both are determined from deptid is file
        df['rt_product_category'] = df['rt_product_type']
        
        # Check if item_size in the dataframe if not then set all rt_item_size to empty string 
        if 'rt_item_size' not in df.columns:
            df['rt_item_size'] = ''
        
        df = df[self.cols]
        return df


class processAdvent(processTiger):
    def __init__(self):
        self.col_names_dict = {
            'sku':'rt_product_id',
            'mainupc':'rt_upc_code',
            'itemname':'rt_brand_name',
            'description':'rt_brand_description',
            'depid':'rt_product_type',
            # 'depid':'rt_product_category',
            'keyword':'rt_package_size', # are we sure about this???
            'priceperunit':'price_regular',
            'currentcost':'price_sale', # This should be CURRENTCOST or ISSERIALIZED
            'instoreqty':'qty_on_hand'
        }
        pass

    def process_data(self, df):
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        # Drop row where no product_id is provided (maybe not the case where it has to be a digit)
        # if product_id can not be a digit then change this to simply drop the first row
        df = df[df['rt_product_id'].apply(lambda x: str(x).isdigit())]

        # Get rt_package_size from first element of string (seperated by ',') in 'KEYWORD' column in file
        df['rt_package_size'] = df['rt_package_size'].str.split(',').str[0]

        # For both rt_product_type and rt_product_category turn depid into category
        df['rt_product_type'] = df['rt_product_type'].apply(lambda x: 'BEER' if x == 5 \
                                                            else 'LIQUOR' if x == 1 \
                                                            else 'WINE' if x == 2 else 'EXTRAS')

        # set rt_product_category to equal rt_product_type since both are determined from depid is file
        df['rt_product_category'] = df['rt_product_type']
        
        # Check if item_size in the dataframe if not then set all rt_item_size to empty string 
        if 'rt_item_size' not in df.columns:
            df['rt_item_size'] = ''
        
        df = df[self.cols]
        return df


# class processPOSNAME2Pos(processMPower):
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
    print(filename)
    print(str(filename.split('/')[-1].split('_')[0]).lower() + '_')
    retailer_id, pos = retailer_df[retailer_df['filename'].str.lower() == str(filename.split('/')[-1].split('_')[0]).lower() + '_'][['id','pos']].iloc[0]
    return retailer_id, pos
    
def process_pos(input_filenames, output_filename):
    retailer_id, retailer_pos = get_retailer_info(input_filenames[0])
    
    # Check what pos the retailer has in the retailer table on RDS and use the appropriate pos processing class
    if retailer_pos.lower() == 'mpower':
        pos_proc = processMPower()

    elif retailer_pos.lower() == 'tiger':
        pos_proc = processTiger()
        
    elif retailer_pos.lower() == 'advent':
        pos_proc = processAdvent()

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
        download_path = '/tmp/{}_{}'.format(tmpkey, uuid.uuid4())
        upload_path = '/tmp/processed-{}'.format(tmpkey)
        
        # Download file from S3 set as the trigger ('handoff-pos-raw')
        s3_client.download_file(bucket, key, download_path)
        
        # Process data using pos_processing function above
        process_pos([download_path], upload_path)
        
        # Upload processed file to different S3 bucket ('handoff-pos-processed')
        s3_client.upload_file(upload_path, bucket.replace('raw','processed'), "processed_" + key)

    end = time.time()
    print(f'Final lambda runtime: {end - start}(s)')
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }
