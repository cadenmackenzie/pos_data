import json
import time
import os
# from typing import ItemsView
import requests
import pandas as pd
import numpy as np
import json
# from tqdm import tqdm
import math
import boto3
from botocore.exceptions import NoCredentialsError


class ConnectLightSpeedPos(object):
  def __init__(self, retailer_id):
    self.retailer_id = retailer_id
    self.timestamp = None
    self.token = None
    self.store_account_id = None
    self.data_size = 1
    self.limit = 100
    # pull the client_id and client_secret
    self.client_id = '5237c421021a6ad35592f599aa41043f8f15c0dc4c8f745429645200e9d40ff3'
    self.client_secret = '115d5596ee5cf5499b610776d1fa60cd3a22584f81a1930396a0be01a08da64e'
    self.configure()
    pass


  def configure(self):
    # pull retailer table to get shopID and the refresh token, filename, and get timestamp of last pos upload
    response = requests.get("https://register.handofftech.com/v2/util/retailers?apiKey=836804e3-928a-4454-b064-485848cc6336") # TODO: get endpoint from Caden --> pull retailer info and match with filename
    print('Pulling Retailer Info -> Reponse Status: ', response.status_code)
    if response.status_code == 200:

      try:
        self.retailer_id = int(self.retailer_id)
      except:
        print('Error retailer_id cannot be turned into an integer')
      # Read return into pandas dataframe
      retailer_df = pd.DataFrame.from_dict(response.json()['data'])
      retailer = retailer_df[retailer_df['id'] == self.retailer_id]

      self.shopID = str(retailer['shopID'].tolist()[0])
      self.refresh_token = str(retailer['refreshToken'].tolist()[0])
      self.filename = str(retailer['filename'].astype(str))
      
      self.get_token()

    else:
      print("ERROR: can't retrive retailer info.")
      print(response.json())
    pass


  def get_account_id(self):
    headers = {
      'Authorization': f'Bearer {self.token}',
    }

    response = requests.get('https://api.lightspeedapp.com/API/Account.json', headers=headers)

    if response.status_code == 200:
        self.store_account_id = response.json()['Account']['accountID']
      
    else:
      print("ERROR: can't retrive AccountID.")
      print(response.json())
    return 


  def get_token(self):
    '''
    Get access token for API access
    '''
    if self.token == None:
      print('Retrieving Token...')
      files = {
        'refresh_token': (None, self.refresh_token),
        'client_id': (None, self.client_id),
        'client_secret': (None, self.client_secret),
        'grant_type': (None, 'refresh_token'),
      }

      response = requests.post('https://cloud.lightspeedapp.com/oauth/access_token.php', files=files)

      if response.status_code == 200:
        self.token = response.json()['access_token']
        print('Successful.')
      
      else:
        print("ERROR: can't retrive token.")
        print(response.json())

    else:
      print('Token is not expired')
      pass


  def get_item_data(self, offset):
    '''
    Get request to get pull item information from Lightspeed API.
    '''
    if self.timestamp:
      url = f"https://api.lightspeedapp.com/API/Account/{self.store_account_id}/Item.json?load_relations=[\"ItemShops\"]&offset={offset}&limit={self.limit}&ItemShops.timeStamp=>,{self.timestamp}"
    else:
      url = f"https://api.lightspeedapp.com/API/Account/{self.store_account_id}/Item.json?load_relations=[\"ItemShops\"]&offset={offset}&limit={self.limit}"

    payload={}
    headers = {
      'Authorization': f'Bearer {self.token}',
      'Cookie': '__cfduid=d498310fc504c6b83c1d8cfe84b62015d1620251698'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 401:
      print(response.json())
      self.token = None
      return

    elif response.status_code == 200:
      self.data_size = int(response.json()['@attributes']['count'])
      # print(response.json())
      # Read in items
      df = pd.json_normalize(response.json()['Item'])
      item_df = df[[x for x in df.columns if x != 'Prices.ItemPrice' and x != 'ItemShops.ItemShop']]
      
      # Get price for items
      price_df = pd.json_normalize(response.json()['Item'], record_path=['Prices','ItemPrice'], meta=['itemID'])
      price_df = price_df[price_df['useTypeID'] == self.shopID]

      quantity_df = pd.json_normalize(response.json()['Item'], record_path=['ItemShops','ItemShop'])
      quantity_df = quantity_df[quantity_df['shopID'] == self.shopID]

      df = pd.merge(item_df, price_df, on=['itemID'])
      df = pd.merge(df, quantity_df, on=['itemID'])
      return df
    
    else:
      print('ERROR')
      print(response.json())
      return


  def get_category_data(self, offset):
    '''
    Get request to get pull category information from Lightspeed API.
    '''
    url = f"https://api.lightspeedapp.com/API/Account/{self.store_account_id}/Category.json?offset={offset}&limit={self.limit}"

    payload={}
    headers = {
      'Authorization': f'Bearer {self.token}',
      'Cookie': '__cfduid=d498310fc504c6b83c1d8cfe84b62015d1620251698'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 401:
      print(response.json())
      self.token = None
      return

    elif response.status_code == 200:
      self.data_size = int(response.json()['@attributes']['count'])
      # print(response.json())
      # Read in items
      category_df = pd.json_normalize(response.json()['Category'])
      return category_df
    
    else:
      print('ERROR')
      print(response.json())
      return


  def get_data_iterator(self, func):
    offset = 0
    data = []

    # Get item info
    while offset < self.data_size:
      print(f'offset {offset}, data_size:{self.data_size}')
      self.get_token()
      self.get_account_id()

      partial_data_df = func(offset=offset)

      if isinstance(partial_data_df, pd.DataFrame) and len(partial_data_df) > 0:
        data.append(partial_data_df)
        offset += self.limit
      else:
        continue
    return pd.concat(data, axis=0)


  def build_csv(self):
    item_data = self.get_data_iterator(self.get_item_data)
    cat_data = self.get_data_iterator(self.get_category_data)
    
    df = pd.merge(item_data, cat_data, on=['categoryID'], how='left')
    df.to_csv(f'/tmp/+{self.filename}', index=False)
    return


  def upload_to_aws(self, local_file, bucket, s3_file):
    print('uploading file')
    try:
      print('connect to S3 client')
      s3_client = boto3.client('s3')
    except:
      s3_client = boto3.client('s3', aws_access_key_id=os.environ(['HANDOFF_AWS_ACCESS_KEY']),
                        aws_secret_access_key=os.environ(['HANDOFF_AWS_SECRET_KEY']))
      

    try:
      s3_client.upload_file(local_file, bucket, s3_file)
      print(f"{local_file} -> {bucket}/{s3_file} Upload Successful.")
      return True
    except FileNotFoundError:
      print("The file was not found")
      return False
    except NoCredentialsError:
      print("Credentials not available")
      return False


  def main(self):
    self.build_csv()
    self.upload_to_aws(local_file=f'/tmp/+{self.filename}', bucket='handoff-pos-raw', s3_file=self.filename)
    return

class ConnectSquarePos(ConnectLightSpeedPos):
  def __init__(self, retailer_id):
    super(ConnectSquarePos, self).__init__(retailer_id)
    self.retailer_id = retailer_id
    self.timestamp = None
    self.token = None
    # pull the client_id and client_secret
    self.client_id = 'sq0idp-89PGcygTFfFCrmOpzGoF5g'
    self.client_secret = 'sq0csp-1RTykI-Ta_amx9JWASZpIXbbnw3fDqiaVRLljnsonrU'
    self.configure()
    pass

  def get_token(self):
    '''
    Get access token for API access
    '''
    if self.token == None:
      print('Retrieving Token...')
      headers = {
          'Square-Version': '2021-06-16',
          'Content-Type': 'application/json',
      }

      data = '{ "grant_type": "refresh_token", "refresh_token": "'+str(self.refresh_token) +'", "client_id": "' + str(self.client_id) + '", "client_secret": "' + str(self.client_secret) + '" }'

      response = requests.post('https://connect.squareup.com/oauth2/token', headers=headers, data=data)

      if response.status_code == 200:
        print(response.json())
        response = response.json()
        self.token = response['access_token']
        print(self.token)
        self.refresh_token = response['refresh_token']
        return

      else:
        print("ERROR: can't retrive token.")
        print(response.json())
        return

    else:
      print('Token is not expired')
      pass

  def get_item_data(self, offset):
    '''
    Get request to get pull item information from Sqaure API.
    '''
    headers = {
        'Square-Version': '2021-06-16',
        'Authorization': f'Bearer {self.token}',
        'Content-Type': 'application/json',
    }

    if offset == "":
      params = (
        ('types', 'ITEM'),
      )
    else:
      params = (
        ('types', 'ITEM'),
        ('cursor', f'{offset}'),
      )

    response = requests.get('https://connect.squareup.com/v2/catalog/list', headers=headers, params=params)

    if response.status_code == 401:
      print(response.json())
      self.token = None
      return

    elif response.status_code == 200:
      response = response.json()

      if 'cursor' in response.keys():
        cursor = response['cursor']
      else:
        cursor = None

      item_df = pd.json_normalize(response['objects'], 
        meta=['id'],
        errors='ignore'
      )

      return item_df, cursor
    
    else:
      print('ERROR')
      print(response.json())
      return

  def get_sku_data(self, offset):
    '''
    Get request to get pull item information from Sqaure API.
    '''
    headers = {
    'Square-Version': '2021-06-16',
    'Authorization': f'Bearer {self.token}',
    'Content-Type': 'application/json',
    }

    if offset == "":
      params = (
        ('types', 'ITEM_VARIATION'),
      )
    else:
      params = (
        ('types', 'ITEM_VARIATION'),
        ('cursor', f'{offset}'),
      )

    response = requests.get('https://connect.squareup.com/v2/catalog/list', headers=headers, params=params)

    if response.status_code == 401:
      print(response.json())
      self.token = None
      return

    elif response.status_code == 200:
      response = response.json()

      if 'cursor' in response.keys():
        cursor = response['cursor']
      else:
        cursor = None

      sku_df = pd.json_normalize(response['objects'], 
        errors='ignore'
      )
      sku_df.rename(columns={'id':'item_variation_object_id'}, inplace=True)
      return sku_df, cursor
    
    else:
      print('ERROR')
      print(response.json())
      return

  def get_category_data(self, offset):
    '''
    Get request to get pull item information from Sqaure API.
    '''
    headers = {
    'Square-Version': '2021-06-16',
    'Authorization': f'Bearer {self.token}',
    'Content-Type': 'application/json',
    }

    if offset == "":
      params = (
      ('types', 'CATEGORY'),
    )

    else:
      params = (
        ('types', 'CATEGORY'),
        ('cursor', f'{offset}'),
      )

    response = requests.get('https://connect.squareup.com/v2/catalog/list', headers=headers, params=params)

    if response.status_code == 401:
      print(response.json())
      self.token = None
      return

    elif response.status_code == 200:
      response = response.json()

      if 'cursor' in response.keys():
        cursor = response['cursor']
      else:
        cursor = None

      cat_df = pd.json_normalize(response['objects'], 
        errors='ignore'
      )
      cat_df.rename(columns={'id':'category_object_id'}, inplace=True)
      return cat_df, cursor
    
    else:
      print('ERROR')
      print(response.json())
      return

  def get_quantity_data(self, offset):
    '''
    Get request to get pull item quanity from Square POS API.
    '''
    headers = {
        'Square-Version': '2021-06-16',
        'Authorization': f'Bearer {self.token}',
        'Content-Type': 'application/json',
    }

    data = '{ "location_ids": [ "'+ str(self.shopID) +'" ], "states": ["IN_STOCK"], "cursor": "'+ str(offset) +'" }'

    response = requests.post('https://connect.squareup.com/v2/inventory/batch-retrieve-counts', headers=headers, data=data)

    if response.status_code == 401:
      print(response.json())
      self.token = None
      return

    elif response.status_code == 200:
      response = response.json()

      if 'cursor' in response.keys():
        cursor = response['cursor']
      else:
        cursor = None

      quantity_df = pd.json_normalize(response['counts'])
      return quantity_df, cursor
    
    else:
      print('ERROR')
      print(response.json())
      return

  def get_data_iterator(self, func):
    offset = ""
    data = []

    # Get item info
    while True:
      print(f'offset {offset}')
      self.get_token()

      partial_data_df, cursor = func(offset=offset)

      if isinstance(partial_data_df, pd.DataFrame) and len(partial_data_df) > 0:
        data.append(partial_data_df)
        offset = cursor

      if cursor == None:
        print(cursor)
        break

    return pd.concat(data, axis=0)

  def build_csv(self):
    item_data = self.get_data_iterator(self.get_item_data)
    category_data = self.get_data_iterator(self.get_category_data)

    df = pd.merge(
      item_data, 
      category_data, 
      left_on=['item_data.category_id'], 
      right_on=['category_object_id'],
    )

    sku_data = self.get_data_iterator(self.get_sku_data)

    df = pd.merge(
      df, 
      sku_data, 
      left_on=['id'], 
      right_on=['item_variation_data.item_id'],
    )

    quantity_df = self.get_data_iterator(self.get_quantity_data)

    df = pd.merge(
      df, 
      quantity_df, 
      left_on=['item_variation_object_id'], 
      right_on=['catalog_object_id'],
    )
    # df.to_csv('square_test.csv')
    df.to_csv(f'/tmp/+{self.filename}', index=False)
    return

class connectMPower(ConnectSquarePos):
  def __init__(self, retailer_id):
    super(connectMPower, self).__init__(retailer_id)
    self.retailer_id = retailer_id
    self.timestamp = None
    self.token = None
    self.configure()
    pass

  def get_token(self):
    '''
    Get access token for API access
    '''
    self.token = self.refresh_token

  def get_data(self, offset):
    url = f"https://mpowerapi.azurewebsites.net/api/v1/Items?pageNumber={offset}&locationId=1"

    payload={}
    headers = {
      'Authorization': self.token,
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    return response

  def get_retail(self, x):
      if not math.isnan(x['Retail_add_upc']):
          return x['Retail_add_upc']
      elif not math.isnan(x['Retail']):
          return x['Retail']
      else:
          return x['CaseRetail']/x['CaseQuantity']

  def get_qtyonhand(self, x):
    if not math.isnan(x['Quantity']):
        return x['QuantityOnHand']/x['Quantity']
    else:
        return x['QuantityOnHand']
      
  def get_size(self, x):
    if not math.isnan(x['Quantity']):
        return str(int(x['Quantity'])) + ' Pack ' + x['Size']
    else:
        return x['Size']

  def standardize(self, df):
    df['product_id'] = df['SkuNumber']

    df['upc'] = df['Upc'].fillna(df['Upc_add_upc'])
    df['upc'].fillna('',inplace=True)

    df['brand_name'] = 

    df['brand_description'] = df['Name']

    df['product_type'] = df['Department']

    df['product_category'] = df['Category']

    df['CasePK'] = df['CaseQuantity']

    df['price_regular'] = df.apply(lambda x: self.get_retail(x), axis=1)
    df['qty_on_hand'] = df.apply(lambda x: self.get_qtyonhand(x), axis=1)
    df['package_size'] = df.apply(lambda x: self.get_size(x), axis=1)

  def build_csv(self):
    dfs = []
    current_page = 1
    total_pages = 100

    while current_page <= total_pages:
      response = self.get_data(offset=current_page)
           
      additional_upcs = pd.json_normalize(response.json()['Results'], 'AdditionalUpcs', ['Id'])
      items = pd.json_normalize(response.json()['Results'])
      
      df = pd.merge(items, additional_upcs, how='outer', on='Id', suffixes=('_item','_add_upc'))
      dfs.append(df)
      
      # Pagination
      total_pages = response.json()['TotalPages']
      current_page += 1
      print(total_pages, current_page, len(dfs))
      
      time.sleep(1)
    
    df = pd.concat(dfs, axis=0)

    df = self.standardize(df)

    df.to_csv(f'/tmp/+{self.filename}', index=False)
    return



def get_pos():
    # pull retailer table to get shopID and the refresh token, filename, and get timestamp of last pos upload
    response = requests.get("https://register.handofftech.com/v2/util/retailers?apiKey=836804e3-928a-4454-b064-485848cc6336") # TODO: get endpoint from Caden --> pull retailer info and match with filename
    print('Pulling Retailer Info -> Reponse Status: ', response.status_code)
    if response.status_code == 200:
      # Read return into pandas dataframe
      retailer_df = pd.DataFrame.from_dict(response.json()['data'])
  
      return retailer_df
    
def lambda_handler(event, context):
    start = time.time()
        
    retailer_df = get_pos()

    # Configure POS and retailer_id
    if 'lightspeed' in retailer_df['pos'].str.lower().tolist():
      retailer_ids = retailer_df[retailer_df['pos'].str.lower() == 'lightspeed']['id'].tolist()
      for r_id in retailer_ids:
        print(f'retailer_id: {r_id} uses lightspeed pos system')
        connect_api = ConnectLightSpeedPos(retailer_id=r_id)
        connect_api.main()

    if 'square' in retailer_df['pos'].str.lower().tolist():
      retailer_ids = retailer_df[retailer_df['pos'].str.lower() == 'square']['id'].tolist()
      for r_id in retailer_ids:
        print(f'retailer_id: {r_id} uses square pos system')
        connect_api = ConnectSquarePos(retailer_id=r_id)
        connect_api.main()

    if 'mpower' in retailer_df['pos'].str.lower().tolist():
      retailer_ids = retailer_df[(retailer_df['pos'].str.lower() == 'mpower') & (retailer_df['refreshToken'].str.lower().notnull())]['id'].tolist()
      print(retailer_ids)
      for r_id in retailer_ids:
        print(f'retailer_id: {r_id} uses square pos system')
        connect_api = connectMPower(retailer_id=r_id)
        connect_api.main()
    
    print('Function Complete')
    end = time.time()
    print(f'Final lambda runtime: {end - start}(s)')
    return {
        'statusCode': 200,
        'body': json.dumps('Pos pull successful')
    }

if __name__ == "__main__":
  get_pos()
  mp = connectMPower(retailer_id=86)
  mp.build_csv()