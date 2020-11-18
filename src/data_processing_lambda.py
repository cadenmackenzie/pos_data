import sys
import os
import time
import requests
import json
import boto3
from urllib.parse import unquote_plus
from glob import glob
from zipfile import ZipFile
from dbfread import DBF
import datetime
import pandas as pd
from parse_string_class import PackageConfigurationParsing
pd.options.mode.chained_assignment = None  # default='warn'


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
            'sale_price':'price_sale'
        }
        pass

    def _check_data_types(self, df, numeric_cols = ['rt_product_id','price_regular','price_sale','qty_on_hand']):
        for c in df.columns:
            if c in numeric_cols:
                df[c] = pd.to_numeric(df[c], errors='coerce').astype(float)
                df[c] = df[c].fillna(0)
            else:
                df[c] = df[c].fillna('').astype(str)
        return df

    def load_data(self, input_filenames):
        if '.csv' in input_filenames:
            df = pd.read_csv(input_filenames) # read in filename as str
            return df
        else:
            raise Exception("Unrecognized file type - expecting .csv or zipped .csv.")
        pass

    def process_data(self, df):
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)
        # Drop row where no product_id is provided (maybe not the case where it has to be a digit)
        # if product_id can not be a digit then change this to simply drop the first row
        df = df[df['rt_product_id'].apply(lambda x: str(x).isdigit())]

        # Check if price_sale in the dataframe if not then set all price_sale to 0
        if 'price_sale' not in df.columns: 
            df['price_sale'] = 0
        
        # Check if item_size in the dataframe if not then set all rt_item_size to empty string 
        if 'rt_item_size' not in df.columns:
            df['rt_item_size'] = ''

        df = self._check_data_types(df)
        df = df[self.cols]
        return df


class processTiger(processMPower):
    def __init__(self):
        super(processTiger, self).__init__()
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
        if '.csv' in input_filenames:
            df = pd.read_csv(input_filenames, sep='|', encoding='ISO-8859-1') # read in filename as str using | as delimiter
            return df
        else:
            raise Exception("Unrecognized file type - expecting .csv or zipped .csv.")
        pass

    def process_data(self, df):
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        # Drop row where no product_id is provided (maybe not the case where it has to be a digit)
        # if product_id can not be a digit then change this to simply drop the first row
        df = df[df['rt_product_id'].apply(lambda x: str(x).isdigit())]

        # For both rt_product_type and rt_product_category turn deptid into category
        df['rt_product_type'] = df['rt_product_type'].astype(str).apply(lambda x: 'BEER' if x == '3' \
                                                                        else 'LIQUOR' if x == '2' \
                                                                        else 'WINE' if x == '4' \
                                                                        else 'EXTRAS')

        # set rt_product_category to equal rt_product_type since both are determined from deptid is file
        df['rt_product_category'] = df['rt_product_type']
        
        # Check if item_size in the dataframe if not then set all rt_item_size to empty string 
        if 'rt_item_size' not in df.columns:
            df['rt_item_size'] = ''
        
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


class processAdvent(processTiger):
    def __init__(self):
        super(processAdvent, self).__init__()
        self.col_names_dict = {
            'sku':'rt_product_id',
            'mainupc':'rt_upc_code',
            'itemname':'rt_brand_name',
            'description':'rt_brand_description',
            'depid':'rt_product_type',
            # 'depid':'rt_product_category',
            'keyword':'rt_package_size', # are we sure about this???
            'priceperunit':'price_regular',
            'isserialized':'price_sale', # This should be CURRENTCOST or ISSERIALIZED
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
        df['rt_product_type'] = df['rt_product_type'].astype(str).apply(lambda x: 'BEER' if x == '5' \
                                                                        else 'LIQUOR' if x == '1' \
                                                                        else 'WINE' if x == '2' \
                                                                        else 'EXTRAS')

        # set rt_product_category to equal rt_product_type since both are determined from depid is file
        df['rt_product_category'] = df['rt_product_type']

        # Fill in sale price with 0 if it is not a digit
        df['price_sale'] = pd.to_numeric(df['price_sale'], errors='coerce')
        df['price_sale'] = df['price_sale'].fillna(0).astype(float)
        
        # Check if item_size in the dataframe if not then set all rt_item_size to empty string 
        if 'rt_item_size' not in df.columns:
            df['rt_item_size'] = ''
        
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


class processLiquorPos(processMPower):
    def __init__(self):
        super(processLiquorPos, self).__init__()
        self.col_names_dict = {
            'code_num':'rt_product_id',
            'barcode':'rt_upc_code',
            'brand':'rt_brand_name',
            'descrip':'rt_brand_description',
            'typenam':'rt_product_type',
            # 'typenam':'rt_product_category',
            'size':'rt_package_size',
            'price':'price_regular',
            'qty_on_hnd':'qty_on_hand'
        }
        pass

    def read_dbf_files(self, input_filename): #Reads in DBF files and returns Pandas DF
        '''
        Arguments
        ---------
        dbfile  : DBF file - Input to be imported
        adapted from: https://stackoverflow.com/questions/41898561/pandas-transform-a-dbf-table-into-a-dataframe
        '''
        dbf = DBF(input_filename, ignore_missing_memofile=True)
        df = pd.DataFrame(iter(dbf))
        return df

    def extract_files(self, file_path):
        '''
        Adapted from https://stackoverflow.com/questions/56786321/read-multiple-csv-files-zipped-in-one-file
        '''
        with ZipFile(file_path, "r") as z:
            z.extractall("/tmp/")
        unzipped_path = file_path.replace('/var/task/','/tmp/').replace('.zip','')
        file_paths = glob(unzipped_path+'/*.dbf')
        return file_paths

    def load_data(self, input_filenames):
        count = 0
        if '.zip' in input_filenames:
            # Read .zip file
            file_paths = self.extract_files(input_filenames)
            # iterate through files in .zip file and read into pandas dataframe
            for f in file_paths:
                if '/BARCODES.dbf' in f:
                    df = self.read_dbf_files(f)
                    df = df[['CODE_NUM','BARCODE']]
                elif '/LIQCODE.dbf' in f:
                    df = self.read_dbf_files(f)
                    df.drop(['BARCODE'], axis=1, inplace=True)
                else:
                    continue

                if count == 0:
                    final_df = df
                elif count > 0:
                    if 'CODE_NUM' in final_df.columns and 'CODE_NUM' in df.columns:
                        final_df = final_df.merge(df, on=['CODE_NUM'])
                    else:
                        raise Exception("Can't find unique key - expecting 'CODE_NUM' to be unique key.")
                count += 1
        else:
            raise Exception("Unrecognized file type - expecting .zip of .dbf files")
        return final_df

    def process_data(self, df):
        df.columns = df.columns.str.lower() # lower case column names
        df.rename(columns=self.col_names_dict, inplace=True) # rename columns to standard naming for inventoryExtension

        # Drop row where no product_id is provided (maybe not the case where it has to be a digit)
        # if product_id can not be a digit then change this to simply drop the first row
        df = df[df['rt_product_id'].apply(lambda x: str(x).isdigit())]

        # make rt_product_category == rt_product_type since pulling from same column in file
        df['rt_product_category'] = df['rt_product_type']

        # Create rt_brand_description as combo of brand and descrip
        df['rt_brand_description'] = df['rt_brand_name'].astype(str) + " " + df['rt_brand_description'].astype(str)

        # # Fill np.nan qty_on_hand with 0
        # df['qty_on_hand'] = df['qty_on_hand'].fillna(0)
        # df['qty_on_hand'] = df['qty_on_hand'].apply(lambda x: float(x) if str(x).isdigit() else 0) # replace non-digits with zero

        # Check if sale_price in the dataframe if not then set all sale_price to 0
        if 'price_sale' not in df.columns: 
            df['price_sale'] = 0
        
        # Check if item_size in the dataframe if not then set all item_size to empty string 
        if 'rt_item_size' not in df.columns:
            df['rt_item_size'] = ''
        
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


def get_retailer_info(filename):
    start = time.time()
    print('Pulling Retailer Info')
    r = requests.get("https://development.handofftech.com/v2/util/retailers?apiKey=836804e3-928a-4454-b064-485848cc6336") # TODO: get endpoint from Caden --> pull retailer info and match with filename
    print('Reponse Status: ', r)
    # Read return into pandas dataframe
    retailer_df = pd.DataFrame.from_dict(r.json()['data'])
    end = time.time()
    print('Read from database time: {}(s)'.format(end - start))

    # Use the filename to retrieve info POS and retailer_id of retailer --> filename must be unique
    try:
        # remove /tmp/ from input_filename
        filename = str(filename).split('/')[-1]
        retailer_id, pos, retailer_name = retailer_df[retailer_df['filename'].str.lower() == str(filename).lower()][['id','pos','name']].iloc[0]
        print(f"Filename:{str(filename).lower()} found info for retailer:\n\tRetailer Name: {retailer_name}, Retailer ID: {retailer_id}, POS system: {pos}")
        return retailer_id, pos

    # Throw exception if filename is not found in reatiler_df
    except:
        raise Exception(f"[ERROR]: Filename '{str(filename).lower()}' not found. Please make sure {str(filename).lower()} with reatailer_id and pos is a value in retailer table in DB.")
    
def process_pos(input_filename, output_filename):
    # pass input_filename to get_retailer_info to get retailer_id and retailer pos info
    retailer_id, retailer_pos = get_retailer_info(input_filename)

    # Check what pos the retailer has in the retailer table on RDS and use the appropriate pos processing class
    if retailer_pos.lower() == 'mpower':
        pos_proc = processMPower()

    elif retailer_pos.lower() == 'tiger':
        pos_proc = processTiger()
        
    elif retailer_pos.lower() == 'advent':
        pos_proc = processAdvent()

    elif retailer_pos.lower() == 'liquorpos':
        pos_proc = processLiquorPos()

    start = time.time()
    df = pos_proc.load_data(input_filename) # load function for specific POS system
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
        download_path = '/tmp/{}'.format(tmpkey)
        upload_path = '/tmp/processed-{}'.format(tmpkey).replace('.zip','.csv')

        print(f'{download_path} has been loaded to {bucket}.')
        
        # Download file from S3 set as the trigger ('handoff-pos-raw')
        s3_client.download_file(bucket, key, download_path)
        
        # Process data using pos_processing function above
        process_pos(download_path, upload_path)
        
        # Upload processed file to different S3 bucket ('handoff-pos-processed')
        date_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        s3_client.upload_file(upload_path, bucket.replace('raw','processed'), date_time+ "-" + key.replace('.zip','.csv'))

    print('Function Complete')
    end = time.time()
    print(f'Final lambda runtime: {end - start}(s)')
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }
