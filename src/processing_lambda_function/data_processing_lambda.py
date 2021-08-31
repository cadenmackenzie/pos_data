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
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from package_configuration_class import PackageConfigurationParser
pd.options.mode.chained_assignment = None  # default='warn'

print('Loading Function')
s3_client = boto3.client('s3')

class processMPower(object):
    def __init__(self):
        self.cols = [
            'rt_product_id','rt_upc_code','rt_brand_name',
            'rt_brand_description','rt_product_type','rt_product_category',
            'rt_package_size','rt_item_size','price_regular',
            'price_sale','qty_on_hand','wholesale_package_size'
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

    def _check_data_types(self, df, numeric_cols = ['price_regular','price_sale','qty_on_hand']):
        for c in df.columns:
            if c in numeric_cols:
                df[c] = pd.to_numeric(df[c], errors='coerce').astype(float)
                df[c] = df[c].fillna(0)
            elif c in ['rt_upc_code']:
                df[c] = df[c].fillna('').astype(str).str.rstrip('.0')
            else:
                df[c] = df[c].fillna('').astype(str)
        return df

    def _clean_up(self, df):
        # Check if non-required columns in the dataframe if not then set to empty string 
        if 'rt_item_size' not in df.columns:
            df['rt_item_size'] = ''
        if 'rt_brand_name' not in df.columns:
            df['rt_brand_name'] = ''
        if 'rt_package_size' not in df.columns:
            df['rt_package_size'] = ''
        if 'rt_product_category' not in df.columns:
            df['rt_product_category'] = ''
        if 'rt_product_type' not in df.columns:
            df['rt_product_type'] = ''
        if 'rt_upc_code' not in df.columns:
            df['rt_upc_code'] = ''
        if 'wholesale_package_size' not in df.columns:
            df['wholesale_package_size'] = ''
        # Check if price_sale in the dataframe if not then set all price_sale to 0
        if 'price_sale' not in df.columns:
            df['price_sale'] = 0

        df = df[df['price_regular'] != 0]
        df.drop_duplicates(subset=['rt_product_id'], inplace=True)
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
        
        df = self._clean_up(df)
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
            'catgid':'rt_product_category',
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
        # if '.csv' in input_filenames:
        #     df = pd.read_csv(input_filenames) # read in filename as str
        #     return df
        else:
            raise Exception("Unrecognized file type - expecting .csv or zipped .csv.")
        pass

    def process_data(self, df):
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        # Drop row where no product_id is provided (maybe not the case where it has to be a digit)
        # if product_id can not be a digit then change this to simply drop the first row
        df['rt_product_id'] = df['rt_product_id'].astype(str)

        # For both rt_product_type and rt_product_category turn deptid into category
        df['rt_product_type'] = df['rt_product_type'].astype(str).apply(lambda x: 'BEER' if x == '3.0' \
                                                                        else 'LIQUOR' if x == '2.0' \
                                                                        else 'WINE' if x == '4.0' \
                                                                        else 'EXTRAS')
        
        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df



class processAdvent(processTiger):
    def __init__(self):
        super(processAdvent, self).__init__()
        self.col_names_dict = {
            0:'rt_product_id',
            4:'rt_upc_code',
            1:'rt_brand_name',
            3:'rt_brand_description',
            27:'rt_product_type',
            # 'depid':'rt_product_category',
            96:'rt_package_size', # are we sure about this???
            13:'price_regular',
            54:'price_sale', # This should be CURRENTCOST or ISSERIALIZED
            19:'qty_on_hand'
        }
        pass

    def load_data(self, input_filenames):        
        if '.csv' in input_filenames:
            df = pd.read_csv(input_filenames, sep='|', encoding='ISO-8859-1', skiprows=4, header=None) # read in filename as str using | as delimiter
            return df
        else:
            raise Exception("Unrecognized file type - expecting .csv or zipped .csv.")
        pass

    def process_data(self, df):
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
        
        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


class processCashRegisterExpress(processMPower):
    def __init__(self):
        super(processCashRegisterExpress, self).__init__()
        self.col_names_dict = {
            0:'rt_upc_code',
            1:'rt_brand_description',
            4:'price_regular',
            5:'price_sale',
            6:'qty_on_hand',
            13:'rt_product_type',
            27:'rt_product_category',
            36:'rt_package_size',
            90:'rt_product_id',
            # # 'depid':'rt_product_category',            
        }
        pass

    def load_data(self, input_filenames):        
        if '.csv' in input_filenames:
            df = pd.read_csv(input_filenames, sep='|', header=None, skiprows=5, error_bad_lines=False, engine='python') # read in filename as str using | as delimiter
            return df
        else:
            raise Exception("Unrecognized file type - expecting .csv or zipped .csv.")
        pass

    def process_data(self, df):
        df.rename(columns=self.col_names_dict, inplace=True)

        # Drop row where no product_id is provided (maybe not the case where it has to be a digit)
        # if product_id can not be a digit then change this to simply drop the first row
        df['rt_product_id'] = df['rt_product_id'].apply(lambda x: str(x).replace('-',''))        

        # Fill in sale price with 0 if it is not a digit
        df['price_sale'] = pd.to_numeric(df['price_sale'], errors='coerce')
        df['price_sale'] = df['price_sale'].fillna(0).astype(float)
        
        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


class processCashRegisterExpress_v2(processMPower):
    def __init__(self):
        super(processCashRegisterExpress_v2, self).__init__()
        self.col_names_dict = {
            0:'rt_upc_code',
            1:'rt_brand_description',
            4:'price_regular',
            100:'price_sale',
            6:'qty_on_hand',
            94:'rt_product_type',
            13:'rt_product_category',
            36:'rt_package_size',
            90:'rt_product_id',
            # 36:'rt_item_size',
        }
        pass

    def load_data(self, input_filenames):        
        if '.csv' in input_filenames:
            # df = pd.read_csv(input_filenames, sep='|', error_bad_lines=False, engine='python') # read in filename as str using | as delimiter
            df = pd.read_csv(input_filenames, sep='|', header=None, skiprows=5, error_bad_lines=False, engine='python') # read in filename as str using | as delimiter
            df = df.iloc[:-1]
            return df
        else:
            raise Exception("Unrecognized file type - expecting .csv or zipped .csv.")
        pass

    def process_data(self, df):
        df.rename(columns=self.col_names_dict, inplace=True)

        # Drop row where no product_id is provided (maybe not the case where it has to be a digit)
        # if product_id can not be a digit then change this to simply drop the first row
        df['rt_product_id'] = df['rt_product_id'].apply(lambda x: str(x).replace('-',''))        

        # Fill in sale price with 0 if it is not a digit
        df['price_sale'] = pd.to_numeric(df['price_sale'], errors='coerce')
        df['price_sale'] = df['price_sale'].fillna(0).astype(float)
        
        df = self._clean_up(df)
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
        try: 
            dbf = DBF(input_filename, ignore_missing_memofile=True)
            df = pd.DataFrame(iter(dbf))
        except:
            dbf = DBF(input_filename, ignore_missing_memofile=True)
            dbf.char_decode_errors = 'ignore'
            df = pd.DataFrame(iter(dbf))
        return df

    def extract_files(self, file_path):
        '''
        Adapted from https://stackoverflow.com/questions/56786321/read-multiple-csv-files-zipped-in-one-file
        '''
        with ZipFile(file_path, "r") as z:
            z.extractall("/tmp/")

        unzipped_path = '/tmp/var/www/html/' + file_path.replace('/tmp/','').replace('.zip','')
        
        # print('glob unzipped path: ',glob(unzipped_path))
        
        file_paths = glob(unzipped_path+'/*.dbf')
        return file_paths

    def load_data(self, input_filename):
        print(input_filename)
        count = 0
        if '.zip' in input_filename:
            # Read .zip file
            file_paths = self.extract_files(input_filename)
            print('liquorpos .dbf filepaths: ', file_paths)
            # iterate through files in .zip file and read into pandas dataframe
            for f in file_paths:
                print('filename: ', f)
                if 'barcodes.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df = df[['CODE_NUM','BARCODE']]
                elif 'liqcode.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df.drop(['BARCODE'], axis=1, inplace=True)
                else:
                    pass

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

        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


class processSpirit2000_dbf(processLiquorPos):
    def __init__(self):
        super(processSpirit2000_dbf, self).__init__()
        self.col_names_dict = {
            'sku':'rt_product_id',
            'upc':'rt_upc_code',
            'name':'rt_brand_description',
            # 'catname':'rt_product_type',
            'typename':'rt_product_type',
            'qty':'rt_package_size',
            'sname':'rt_item_size',
            'price':'price_regular',
            'sale':'price_sale',
            'back':'qty_on_hand',
        }
        pass

    def read_dbf_files(self, input_filename): #Reads in DBF files and returns Pandas DF
        '''
        Arguments
        ---------
        dbfile  : DBF file - Input to be imported
        adapted from: https://stackoverflow.com/questions/41898561/pandas-transform-a-dbf-table-into-a-dataframe
        '''
        try:
            dbf = DBF(input_filename, ignore_missing_memofile=True)
            df = pd.DataFrame(iter(dbf))
        except:
            from dbfread import DBF, FieldParser, InvalidValue

            class MyFieldParser(FieldParser):
                def parse(self, field, data):
                    try:
                        return FieldParser.parse(self, field, data)
                    except ValueError:
                        return InvalidValue(data)

            dbf = DBF(input_filename, ignore_missing_memofile=True, parserclass=MyFieldParser)
            dbf.char_decode_errors = 'ignore'
            df = pd.DataFrame(iter(dbf))
        return df

    def load_data(self, input_filename):
        count = 0
        if '.zip' in input_filename:
            # Read .zip file
            file_paths = self.extract_files(input_filename)
            print('Spirit 2000 .dbf filepaths: ', file_paths)
            # iterate through files in .zip file and read into pandas dataframe
            for f in file_paths:
                print('filename: ', f)
                if 'inv.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df = df[['SKU','NAME','SNAME','ML','PACK','SDATE','TYPENAME','WEBSENT','SENT']]
                    # Aggregating on SKU, size, package size and wholesale package size and taking the maximum date

                elif 'prc.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df = df [['SKU','QTY','PRICE','SALE','ONSALE','WHO','LEVEL','TSTAMP']]

                    df['TSTAMP'] = pd.to_datetime(df['TSTAMP'])
                    df = df[df['LEVEL'] == '1']

                    idx = df.groupby(['SKU'])['QTY'].transform(max) == df['QTY']
                    df = df[idx]
                    idx = df.groupby(['SKU'])['TSTAMP'].transform(max) == df['TSTAMP']
                    df = df[idx]

                    df = df [['SKU','QTY','PRICE','SALE','ONSALE','WHO','LEVEL']]

                elif 'stk.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df = df[['SKU','BACK','TSTAMP']]

                    df['TSTAMP'] = pd.to_datetime(df['TSTAMP'], )

                    idx = df.groupby(['SKU'])['TSTAMP'].transform(max) == df['TSTAMP']
                    df = df[idx]
                    df = df[['SKU','BACK']]

                elif 'upc.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df = df[['SKU','UPC','TSTAMP']]

                    df['TSTAMP'] = pd.to_datetime(df['TSTAMP'])

                    idx = df.groupby(['SKU'])['TSTAMP'].transform(max) == df['TSTAMP']
                    df = df[idx]
                    df = df[['SKU','UPC']]
                else:
                    pass

                if count == 0:
                    final_df = df
                elif count > 0:
                    if 'SKU' in final_df.columns and 'SKU' in df.columns:
                        final_df = final_df.merge(df, on=['SKU'])
                    else:
                        raise Exception("Can't find unique key - expecting 'SKU' to be unique key.")
                count += 1
        else:
            raise Exception("Unrecognized file type - expecting .zip of .dbf files")
        return final_df

    def process_data(self, df):
        df = df.sort_values('UPC')
        df = df.drop_duplicates('SKU', keep='first')
        # Lower the columns and rename
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        df['rt_product_id'] = df['rt_product_id'].astype(str)
        df['rt_package_size'] = df['rt_package_size'].astype(str) + ' pack'

        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


class processSpirit2000_csv(processSpirit2000_dbf):
    def __init__(self):
        super(processSpirit2000_csv, self).__init__()
        self.col_names_dict = {
            'sku':'rt_product_id',
            'upc':'rt_upc_code',
            'name':'rt_brand_description',
            # 'catname':'rt_product_type',
            'typename':'rt_product_type',
            'qty':'rt_package_size',
            'sname':'rt_item_size',
            'price':'price_regular',
            'sale':'price_sale',
            'back':'qty_on_hand',
            'pack':'wholesale_package_size'
        }
        pass

    def extract_files(self, file_path):
        '''
        Adapted from https://stackoverflow.com/questions/56786321/read-multiple-csv-files-zipped-in-one-file
        '''
        with ZipFile(file_path, "r") as z:
            z.extractall()
        
        print('glob unzipped path: ',glob('tmp/*'))
        
        file_paths = glob('tmp/*.csv')
        print(file_paths)
        return file_paths

    def load_data(self, input_filename):
        count = 0
        if '.zip' in input_filename:
            # Read .zip file
            file_paths = self.extract_files(input_filename)
            print('Spirit 2000.csv filepaths: ', file_paths)
            # iterate through files in .zip file and read into pandas dataframe
            for f in file_paths:
                print('filename: ', f)
                if 'inv.csv' in f.lower():
                    df = pd.read_csv(f)
                    df = df[['sku','name','sname','ml','pack','sdate','typename','websent','sent']]


                elif 'prc.csv' in f.lower():
                    df = pd.read_csv(f)
                    # print(df)
                    df = df[['sku','qty','price','sale','onsale','who','level','tstamp']]

                    df['tstamp'] = pd.to_datetime(df['tstamp'])
                    df['level'] = df['level'].astype(str)
                    df = df[df['level'] == '1']

                    idx = df.groupby(['sku'])['qty'].transform(max) == df['qty']
                    df = df[idx]
                    idx = df.groupby(['sku'])['tstamp'].transform(max) == df['tstamp']
                    df = df[idx]

                    df = df[['sku','qty','price','sale','onsale','who','level']]

                elif 'stk.csv' in f.lower():
                    df = pd.read_csv(f)
                    df = df[['sku','back','tstamp']]

                    df['tstamp'] = pd.to_datetime(df['tstamp'], )

                    idx = df.groupby(['sku'])['tstamp'].transform(max) == df['tstamp']
                    df = df[idx]
                    df = df[['sku','back']]

                elif 'upc.csv' in f.lower():
                    df = pd.read_csv(f)
                    df = df[['sku','upc','tstamp']]

                    df['tstamp'] = pd.to_datetime(df['tstamp'])

                    idx = df.groupby(['sku'])['tstamp'].transform(max) == df['tstamp']
                    df = df[idx]
                    df = df[['sku','upc']]
                else:
                    pass

                if count == 0:
                    final_df = df
                elif count > 0:
                    if 'sku' in final_df.columns and 'sku' in df.columns:
                        final_df['sku'] = final_df['sku'].astype(str)
                        df['sku'] = df['sku'].astype(str)

                        final_df = final_df.merge(df, on=['sku'])
                    else:
                        raise Exception("Can't find unique key - expecting 'sku' to be unique key.")
                count += 1
        else:
            raise Exception("Unrecognized file type - expecting .zip of .csv files")
        return final_df

    def process_data(self, df):
        df = df.sort_values('upc')
        df = df.drop_duplicates('sku', keep='first')
        # Lower the columns and rename
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        df['rt_product_id'] = df['rt_product_id'].astype(str)
        df['rt_package_size'] = df['rt_package_size'].astype(str) + ' pack'
        df['wholesale_package_size'] = df['wholesale_package_size'].astype(str) + ' pack'

        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df
        

class processSpirit2000_xml(processSpirit2000_dbf):
    def __init__(self):
        super(processSpirit2000_xml, self).__init__()
        self.col_names_dict = {
            'sku':'rt_product_id',
            'upccode':'rt_upc_code',
            'prodname':'rt_brand_description',
            'catname':'rt_product_category',
            'typename':'rt_product_type',
            # 'sellqty':'rt_package_size',
            'sizename':'rt_item_size',
            'unitprice':'price_regular',
            'sale':'price_sale',
            'stockqty':'qty_on_hand',
            'pack':'wholesale_package_size',
            'onsale':'onsale'
        }
        pass

    def load_data(self, input_filenames):
        data = {k:[] for k in self.col_names_dict.keys()}
        if '.xml' in input_filenames.lower():
            root = ET.parse(input_filenames).getroot()
            print(root)

            for elem in root:
                if 'webtable' in elem.tag:
                    for k in data.keys():
                        for value in elem.iter(k):
                            data[k] += [value.text]
                        
            df = pd.DataFrame(data)
            return df
        else:
            raise Exception("Unrecognized file type - expecting .xml.")

    def process_data(self, df):
        # Lower the columns and rename
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)
        
        df['rt_product_id'] = df['rt_product_id'].astype(str)
        # df['rt_package_size'] = df['rt_package_size'].astype(str) + ' pack'
        df['wholesale_package_size'] = df['wholesale_package_size'].astype(str) + ' pack'

        # Check if onsale flag is True then set the sale price if the sale price is greater than 0
        df['price_sale'] = df.apply(lambda x: x['price_sale'] if str(x['onsale']) == 'T' and float(x['price_sale']) > 0 else 0.0, axis=1)

        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df

class processSpirit2000_tower(processSpirit2000_dbf):
    def __init__(self):
        super(processSpirit2000_tower, self).__init__()
        self.col_names_dict = {
            'sku':'rt_product_id',
            'upc':'rt_upc_code',
            'name':'rt_brand_description',
            'typename':'rt_product_type',
            'qty':'rt_package_size',
            'sname':'rt_item_size',
            'price':'price_regular',
            'sale':'price_sale',
        }
        pass

    def read_dbf_files(self, input_filename): #Reads in DBF files and returns Pandas DF
        '''
        Arguments
        ---------
        dbfile  : DBF file - Input to be imported
        adapted from: https://stackoverflow.com/questions/41898561/pandas-transform-a-dbf-table-into-a-dataframe
        '''
        try:
            dbf = DBF(input_filename, ignore_missing_memofile=True)
            df = pd.DataFrame(iter(dbf))
        except:
            from dbfread import DBF, FieldParser, InvalidValue

            class MyFieldParser(FieldParser):
                def parse(self, field, data):
                    try:
                        return FieldParser.parse(self, field, data)
                    except ValueError:
                        return InvalidValue(data)

            dbf = DBF(input_filename, ignore_missing_memofile=True, parserclass=MyFieldParser)
            dbf.char_decode_errors = 'ignore'
            df = pd.DataFrame(iter(dbf))
        return df

    def load_data(self, input_filename):
        count = 0
        if '.zip' in input_filename:
            # Read .zip file
            file_paths = self.extract_files(input_filename)
            print('Spirit 2000 .dbf filepaths: ', file_paths)
            # iterate through files in .zip file and read into pandas dataframe
            for f in file_paths:
                print('filename: ', f)
                if 'inv.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df = df[['SKU','NAME','SNAME','ML','PACK','SDATE','TYPENAME','WEBSENT','SENT']]
                    # Aggregating on SKU, size, package size and wholesale package size and taking the maximum date

                elif 'prc.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df = df [['SKU','QTY','PRICE','SALE','ONSALE','WHO','LEVEL','TSTAMP']]

                    df['TSTAMP'] = pd.to_datetime(df['TSTAMP'])

                    # if 'doraville' in f or 'buckhead' in f:
                    df = df[df['LEVEL'] == '7']
                    idx = df.groupby(['SKU'])['TSTAMP'].transform(max) == df['TSTAMP']
                    df = df[idx]
                    df = df [['SKU','QTY','PRICE','SALE','ONSALE','WHO','LEVEL']]

                elif 'stk.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df = df[['SKU','BACK','TSTAMP']]

                    # if 'doraville' in f or 'buckhead' in f:
                    df['TSTAMP'] = pd.to_datetime(df['TSTAMP'], )

                    idx = df.groupby(['SKU'])['TSTAMP'].transform(max) == df['TSTAMP']
                    df = df[idx]
                    df = df[['SKU','BACK']]

                elif 'upc.dbf' in f.lower():
                    df = self.read_dbf_files(f)
                    df = df[['SKU','UPC','LAST']]

                    df['LAST'] = pd.to_datetime(df['LAST'])

                    idx = df.groupby(['SKU'])['LAST'].transform(max) == df['LAST']
                    df = df[idx]
                    df = df[['SKU','UPC']]
                else:
                    pass

                if count == 0:
                    final_df = df
                elif count > 0:
                    if 'SKU' in final_df.columns and 'SKU' in df.columns:
                        final_df = final_df.merge(df, on=['SKU'])
                    else:
                        raise Exception("Can't find unique key - expecting 'SKU' to be unique key.")
                count += 1
        else:
            raise Exception("Unrecognized file type - expecting .zip of .dbf files")
        return final_df

    def process_data(self, df):
        df = df.sort_values('UPC')
        df = df.drop_duplicates('SKU', keep='first')
        df['qty_on_hand'] = df['BACK']/df['QTY']
        # Lower the columns and rename
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)
        
        df['rt_product_id'] = df['rt_product_id'].astype(str)
        df['rt_package_size'] = df['rt_package_size'].astype(str) + ' pack'

        # print(df[df['rt_brand_description'].str.upper().str.contains('CROWN RUSSE VODKA')])
        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


class processLightSpeed(processMPower):
    def __init__(self):
        super(processLightSpeed, self).__init__()
        self.col_names_dict = {
            'itemid':'rt_product_id',
            'upc':'rt_upc_code',
            # '':'rt_brand_name',
            'description':'rt_brand_description',
            'fullpathname':'rt_product_type',
            'name':'rt_product_category',
            # '':'rt_package_size',
            'amount':'price_regular',
            'qoh':'qty_on_hand'
        }

    def process_data(self, df):
        # Lower the columns and rename
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        df['rt_product_id'] = df['rt_product_id'].astype(str)

        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


class processSquare(processMPower):
    def __init__(self):
        super(processSquare, self).__init__()
        self.col_names_dict = {
            'item_variation_object_id':'rt_product_id',
            'item_variation_data.sku':'rt_upc_code',
            # '':'rt_brand_name',
            'item_data.name':'rt_brand_description',
            'item_data.product_type':'rt_product_type',
            'category_data.name':'rt_product_category',
            'item_variation_data.name':'rt_package_size',
            'item_variation_data.price_money.amount':'price_regular',
            'quantity':'qty_on_hand'
        }

    def process_data(self, df):
        # Lower the columns and rename
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        df['price_regular'] = df['price_regular'].apply(lambda x: float(x)/100)
        df['rt_product_id'] = df['rt_product_id'].astype(str)

        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df

class processCobaltConnect(processMPower):
    def __init__(self):
        super(processCobaltConnect, self).__init__()
        self.col_names_dict = {
            'code':'rt_product_id',
            'upc':'rt_upc_code',
            'name':'rt_brand_description',
            'size':'rt_item_size',
            'retail_price':'price_regular',
            'stock_qoh':'qty_on_hand',
            'qtypercase':'wholesale_package_size'
        }

    def clean_wholesale(self, x):
        if str(x).split('.')[0].isdigit():
            return str(int(x)) + ' Pack'
        else:
            return ''
    
    def clean_upc(self, x):
        if str(x)[:2] == 'cc':
            return str(x)[2:]
        else:
            return str(x)

    def process_data(self, df):
        # Lower the columns and rename
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        df['rt_product_id'] = df['rt_product_id'].astype(str)

        # Remove 'cc' letters from beginning of UPC code
        df['rt_upc_code'] = df['rt_upc_code'].apply(lambda x: self.clean_upc(x))

        if 'wholesale_package_size' in df.columns:
            df['wholesale_package_size'] = df['wholesale_package_size'].apply(lambda x: self.clean_wholesale(x))
        # Missing Sale Price

        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df

class processKMDS(processMPower):
    def __init__(self):
        super(processKMDS, self).__init__()
        self.col_names_dict = {
            'id':'rt_product_id',
            'upc':'rt_upc_code',
            'description':'rt_brand_description',
            'price':'price_regular',
            'onhand':'qty_on_hand',
            'unitspercase':'wholesale_package_size',
            'salesdepartmentid':'rt_product_category',
            'description.1':'rt_item_size',
            # 'caseqty':'rt_package_size'
        }

    def load_data(self, input_filenames):        
        if '.csv' in input_filenames:
            df = pd.read_csv(input_filenames, sep='|', encoding='ISO-8859-1') # read in filename as str using | as delimiter
            return df
        else:
            raise Exception("Unrecognized file type - expecting .csv or zipped .csv.")
        pass

    def clean_wholesale(self, x):
        if str(x).split('.')[0].isdigit():
            return str(int(x)) + ' Pack'
        else:
            return ''

    def clean_item_size(self, x):
        try:
            x = float(x)
            if x > 1:
                return str(round(x , 2))+'oz'
        except:
            return x

    def process_data(self, df):
        # Lower the columns and rename
        df.columns = df.columns.str.lower()
        df.rename(columns=self.col_names_dict, inplace=True)

        df = df[df['rt_product_id'].apply(lambda x: x.isnumeric())]
        df['rt_product_id'] = df['rt_product_id'].astype(str)

        df['rt_product_category'] = df['rt_product_category'].astype(str)

        if 'rt_item_size' in df.columns:
            df['rt_item_size'] = df['rt_item_size'].apply(lambda x: self.clean_item_size(x))

        if 'wholesale_package_size' in df.columns:
            df['wholesale_package_size'] = df['wholesale_package_size'].apply(lambda x: self.clean_wholesale(x))

        df = self._clean_up(df)
        df = self._check_data_types(df)
        df = df[self.cols]
        return df


# All POS functions
def get_retailer_info(filename):
    start = time.time()
    
    r = requests.get("https://register.handofftech.com/v2/util/retailers?apiKey=836804e3-928a-4454-b064-485848cc6336") # TODO: get endpoint from Caden --> pull retailer info and match with filename
    print('Pulling Retailer Info -> Reponse Status: ', r)
    # Read return into pandas dataframe
    retailer_df = pd.DataFrame.from_dict(r.json()['data'])
    end = time.time()
    print('Read from database time: {}(s)'.format(end - start))

    # Use the filename to retrieve info POS and retailer_id of retailer --> filename must be unique
    try:
        # remove /tmp/ from input_filename
        filename = str(filename).split('/')[-1]
        retailer_id, pos, retailer_name = retailer_df[retailer_df['filename'].str.lower() == str(filename).lower()][['id','pos','name']].iloc[0]
        print(f"Filename: {str(filename).lower()} found info for retailer -> Retailer Name: {retailer_name}, Retailer ID: {retailer_id}, POS system: {pos}")
        return retailer_id, pos

    # Throw exception if filename is not found in retailer_df
    except:
        raise Exception(f"[ERROR]: Unrecognized filename: '{str(filename).lower()}'. Please make sure {str(filename).lower()} is associated with a retailer_id and pos system in Retailer table in DB.")
    
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

    elif retailer_pos.lower() == 'spirit2000_dbf':
        pos_proc = processSpirit2000_dbf()

    elif retailer_pos.lower() == 'spirit2000_csv':
        pos_proc = processSpirit2000_csv()

    elif retailer_pos.lower() == 'spirit2000_xml':
        pos_proc = processSpirit2000_xml()

    elif retailer_pos.lower() == 'spirit2000_tower':
        pos_proc = processSpirit2000_tower()
    
    elif retailer_pos.lower() == 'cashregisterexpress':
        pos_proc = processCashRegisterExpress()

    elif retailer_pos.lower() == 'cashregisterexpress_v2':
        pos_proc = processCashRegisterExpress_v2()

    elif retailer_pos.lower() == 'lightspeed':
        pos_proc = processLightSpeed()

    elif retailer_pos.lower() == 'cobaltconnect':
        pos_proc = processCobaltConnect()

    elif retailer_pos.lower() == 'kmds':
        pos_proc = processKMDS()

    start = time.time()
    df = pos_proc.load_data(input_filename) # load function for specific POS system
    df = pos_proc.process_data(df) # Processing for specific POS system

    df = PackageConfigurationParser().main(df)
    
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

if __name__ == "__main__":
    # print("Testing processCashRegisterExpress()")
    # proc = processCashRegisterExpress()
    # df = proc.load_data('~/Downloads/square_b_handoff_1.csv')
    # df = proc.process_data(df)
    # print(df.head())
    # print('Saving test_cashregisterexpress.csv')
    # df.to_csv('test_cashregisterexpress.csv')

    # print("Testing processCashRegisterExpress_v2()")
    # proc = processCashRegisterExpress_v2()
    # df = proc.load_data('pecos_inventory.csv')
    # print(df)
    # print(df.shape)
    # print(df.columns)
    # df = proc.process_data(df)
    # print(df.head())
    # print('Saving test_pecos.csv')
    # df.to_csv('test_pecos.csv')

    # print("Testing LiquorPOS() for house_of_spirits")
    # proc = processLiquorPos()
    # df = proc.load_data('house_of_spirits.zip')
    # df = proc.process_data(df)
    # print(df.head())
    # print('Saving test_house_of_spirits.csv')
    # df.to_csv('test_house_of_spirits.csv')

    # print("Testing LiquorPOS() for kingsolomon.zip")
    # proc = processLiquorPos()
    # df = proc.load_data('kingsolomon.zip')
    # df = proc.process_data(df)
    # print(df.head())
    # print(df.shape)
    # print('Saving test_kingsolomon.csv')
    # df.to_csv('test_kingsolomon.csv')

    # print('Testing processSpirit2000() for liquorbarn.zip')
    # proc = processSpirit2000()
    # df = proc.load_data('liquorbarn.zip')
    # print(df)
    # print(df.columns)
    # print(df['SKU'].value_counts())
    # print(df[df['SKU'] == 30010])
    # df = proc.process_data(df)
    # print(df.head())
    # print(df['rt_product_id'].value_counts())
    # print(df.shape)
    # print('Saving test_liquorbarn.csv')
    # df.to_csv('test_liquorbarn.csv')

    # print('Testing processSpirit2000_tower() for doraville.zip')
    # proc = processSpirit2000_tower()
    # df = proc.load_data('doraville.zip')
    # print(df)
    # print(df.columns)
    # print(df['SKU'].value_counts())
    # print(df[df['SKU'] == 50401])
    # df = proc.process_data(df)
    # print(df.head())
    # print(df['rt_product_id'].value_counts())
    # print(df.shape)
    # print('Saving test_doraville.csv')
    # df.to_csv('test_doraville.csv')

    # print('Testing processSpirit2000_tower() for buckhead.zip')
    # proc = processSpirit2000_tower()
    # df = proc.load_data('buckhead.zip')
    # # print(df)
    # # print(df.columns)
    # # print(df['SKU'].value_counts())
    # # print(df[df['SKU'] == 53223])
    # print(df[df['NAME'].str.upper().str.contains('CROWN RUSSE VODKA')][['SKU','NAME','SNAME','ML','PACK','SDATE','UPC','BACK','LEVEL','PRICE']])
    # df = proc.process_data(df)
    # # print(df.head())
    # # print(df['rt_product_id'].value_counts())
    # # print(df.shape)
    # print(df[df['rt_brand_description'].str.upper().str.contains('CROWN RUSSE VODKA')])
    # print('Saving test_buckhead.csv')
    # df.to_csv('test_buckhead.csv')

    # print('Testing processLightSpeed() for Orchard_POS.csv')
    # proc = processLightSpeed()
    # df = proc.load_data('Orchard_POS.csv')
    # df = proc.process_data(df)
    # print('Saving test_Orchard_POS.csv')
    # df.to_csv('test_Orchard_POS.csv')

    # print('Testing processSpirit2000_xml() for GenPrice20201007172428.XML')
    # proc = processSpirit2000_xml()
    # df = proc.load_data('GenPrice20201007172428.XML')
    # df = proc.process_data(df)
    # print('Saving test_GenPrice20201007172428.XML')
    # df.to_csv('test_gen_xml.csv')

    # print('Testing processSquare() for Square_test.csv')
    # proc = processSquare()
    # df = proc.load_data('Square_test.csv')
    # df = proc.process_data(df)
    # print('Saving test_Square_test.csv')
    # df.to_csv('test_Square_test.csv')

    # print('Testing processCobaltConnect() for Windsor_Court.csv')
    # proc = processCobaltConnect()
    # df = proc.load_data('windsor_court.csv')
    # print(df)
    # df = proc.process_data(df)
    # print('Saving test_CobaltConnect.csv')
    # df.to_csv('test_CobaltConnect.csv')

    print('Testing processKMDS() for hillsboro_inventory.csv')
    proc = processKMDS()
    df = proc.load_data('hillsboro_inventory.csv')
    print(df[['Oz','Description','StockNumber','QtyPackPrice','QtyPack','SubPack','UnitsPerPack','UnitsPerOrder','UnitsPerCase','CaseQty']])
    print(df)
    df.to_csv('test_KMDS_unprocessed.csv')
    df = proc.process_data(df)
    print('Saving test_KMDS.csv')
    df.to_csv('test_KMDS.csv')

    # print("Testing TigerPOS() for candc2_handoff.csv")
    # proc = processTiger()
    # df = proc.load_data('candc2_handoff.csv')
    # df = proc.process_data(df)
    # print(df.head())
    # print('Saving test_Tiger.csv')
    # df.to_csv('test_Tiger.csv')

    # print("Testing processSpirit2000_csv() for sweetwater.zip")
    # proc = processSpirit2000_csv()
    # df = proc.load_data('sweetwater.zip')
    # df = proc.process_data(df)
    # print(df.head())
    # print('Saving test_Spiritcsv_sweetwater.csv')
    # df.to_csv('test_Spiritcsv_sweetwater.csv')

    # print("Testing processSpirit2000_csv() for liquorbarn.zip")
    # proc = processSpirit2000_csv()
    # df = proc.load_data('liquorbarn.zip')
    # df = proc.process_data(df)
    # print(df.head())
    # print('Saving test_Spiritcsv_liquorbarn.csv')
    # df.to_csv('test_Spiritcsv_liquorbarn.csv')

    
