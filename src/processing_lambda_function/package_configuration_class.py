import math
import re
import numpy as np
import pandas as pd

class PackageConfigurationParser(object):
    '''
Welcome to the StringParsing Class. This is a class for parsing strings to extract relvant information. 
This class is used in conjunction with the inventory linking classes to match inventory extension to 
master inventory and generate new products. Currently, I extract package size, item size and container 
type information necessary for inventory extension to master extension matching (master_inventory_ext_match_sizes) 
and new product creation (new_product_parse_sizes). Happy Parsing!
    '''
    def __init__(self):
        pass

    #### Regex Logic
    ## * would love to have this cleaned up!
    def regex_rule_package_size(self, string):
        string = str(string).replace('/ PK','PK')
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+/.PK', str(string).upper())
        if m != None:
            try:
                return np.absolute((float(m.group(0).replace('PK','').replace('/','').replace('/PK',''))))
            except:
                print('ERROR: ', m.group(0).replace('PK','').replace('/','').replace('/PK',''))
                return np.nan


        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.-PK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('-PK','').replace('/','')))
            except:
                print('ERROR: ', m.group(0).replace('-PK','').replace('/',''))
                return np.nan
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+-PK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('-PK','').replace('/','')))
            except:
                print('ERROR: ', m.group(0).replace('-PK','').replace('/','').replace('/',''))
                return np.nan

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.-PACK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('-PACK','').replace('/','')))
            except:
                print('ERROR: ', m.group(0).replace('-PACK','').replace('/',''))
                return np.nan
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+-PACK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('-PACK','').replace('/','')))
            except:
                print('ERROR: ', m.group(0).replace('-PACK','').replace('/','').replace('/',''))
                return np.nan

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.PK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('PK','').replace('/','')))
            except:
                print('ERROR: ', m.group(0).replace('PK','').replace('/',''))
                return np.nan
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+PK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('PK','').replace('/','')))
            except:
                print('ERROR: ', m.group(0).replace('PK','').replace('/',''))
                return np.nan
            
        

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+-PCK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('-PCK','')))
            except:
                print('ERROR: ', m.group(0).replace('-PCK',''))
                return np.nan
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.-PCK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('-PCK','')))
            except:
                print('ERROR: ', m.group(0).replace('-PCK',''))
                return np.nan
        
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+PCK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('PCK','')))
            except:
                print('ERROR: ', m.group(0).replace('PCK',''))
                return np.nan
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.PCK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('PCK','')))
            except:
                print('ERROR: ', m.group(0).replace('PCK',''))
                return np.nan

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+-PACK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('-PACK','')))
            except:
                print('ERROR: ', m.group(0).replace('-PACK',''))
                return np.nan
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.-PACK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('-PACK','')))
            except:
                print('ERROR: ', m.group(0).replace('-PACK',''))
                return np.nan

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+PACK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('PACK','')))
            except:
                print('ERROR: ', m.group(0).replace('PACK',''))
                return np.nan
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.PACK', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('PACK','')))
            except:
                print('ERROR: ', m.group(0).replace('PACK',''))
                return np.nan
            
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+PC', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('PC','')))
            except:
                print('ERROR: ', m.group(0).replace('PC',''))
                return np.nan
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.PC', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('PC','')))
            except:
                print('ERROR: ', m.group(0).replace('PC',''))
                return np.nan
            
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+NR', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('NR','')))
            except:
                print('ERROR: ', m.group(0).replace('NR',''))
                return np.nan
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.NR', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('NR','')))
            except:
                print('ERROR: ', m.group(0).replace('NR',''))
                return np.nan

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.BTL', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('BTL','').replace('/','')))
            except:
                print('ERROR: ', m.group(0).replace('BTL','').replace('/',''))
                return np.nan

        m = re.search(r'\d+(?=\/)', str(string).upper())
        if m != None:
            return float(m.group(0))

        if str(string).upper() == 'SINGLE':
            return 1.0
        if str(string).upper() == 'SNG':
            return 1.0
        if str(string).upper() == 'SGL':
            return 1.0
        if str(string).upper() == 'EA':
            return 1.0
        if str(string).upper() == 'KEG':
            return 1.0
        else:
            return np.nan

    def regex_rule_item_size(self, string):
        if '1/16' in str(string).upper():
            string = str(string).upper().replace('1/16','.0625')
        if '1/6' in str(string).upper():
            string = str(string).upper().replace('1/6','.167')
        if '1/8' in str(string).upper():
            string = str(string).upper().replace('1/8','.125')
        if '1/4' in str(string).upper():
            string = str(string).upper().replace('1/4','.25')
        if '1/3' in str(string).upper():
            string = str(string).upper().replace('1/3','.333')
        if '1/2' in str(string).upper():
            string = str(string).upper().replace('1/2','.5')

        m = re.search(r' PINT ', ' ' + str(string).upper() + ' ')
        if m != None:
            return np.nan, 'PINT'
        m = re.search(r' PINT ', ' ' + str(string).upper() + ' ')
        if m != None:
            return np.nan, 'PINT'
        
        m = re.search(r' PT ', ' ' + str(string).upper() + ' ')
        if m != None:
            return np.nan, 'PINT'
        m = re.search(r' PT ', ' ' + str(string).upper() + ' ')
        if m != None:
            return np.nan, 'PINT'

        m = re.search(r' BOMBER ', ' ' + str(string).upper() + ' ')
        if m != None:
            return np.nan, 'BOMBER'
        m = re.search(r' BOMBER ', ' ' + str(string).upper() + ' ')
        if m != None:
            return np.nan, 'BOMBER'
        
        m = re.search(r' BOMB ', ' ' + str(string).upper() + ' ')
        if m != None:
            return np.nan, 'BOMBER'
        m = re.search(r' BOMB ', ' ' + str(string).upper() + ' ')
        if m != None:
            return np.nan, 'BOMBER'
        
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+KEG', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('KEG',''))), 'KEG'
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.KEG', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('KEG',''))), 'KEG'

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+OZ', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('OZ',''))), 'OZ'
            except:
                print('ERROR: ', m.group(0).replace('OZ',''))
                return np.nan, 'OZ'
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.OZ', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('OZ',''))), 'OZ'
            except:
                print('ERROR: ', m.group(0).replace('OZ',''))
                return np.nan, 'OZ'
        
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+ML', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('ML',''))), 'ML'
            except:
                print('ERROR: ', m.group(0).replace('ML',''))
                return np.nan, 'ML'
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.ML', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('ML',''))), 'ML'
            except:
                print('ERROR: ', m.group(0).replace('ML',''))
                return np.nan, 'ML'

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+LBS', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('LBS',''))), 'LB'
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.LBS', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('LBS',''))), 'LB'

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+LB', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('LB',''))), 'LB'
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.LB', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('LB',''))), 'LB'

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+GALLON', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('GALLON',''))), 'GAL'
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.GALLON', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('GALLON',''))), 'GAL'


        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+GAL', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('GAL',''))), 'GAL'
            except:
                print('ERROR: ', m.group(0).replace('GAL',''), 'GAL')
                return np.nan, 'GAL'
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.GAL', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('GAL',''))), 'GAL'
            except:
                print('ERROR: ', m.group(0).replace('GAL',''), 'GAL')
                return np.nan, 'GAL'
        
        m = re.search(r'M+([-+]?[0-9]*\.?[0-9]+)', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('M',''))), 'ML'
        m = re.search(r'.M+([-+]?[0-9]*\.?[0-9]+)', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('M',''))), 'ML'

        # m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+MG', str(string).upper())
        # if m != None:
        #     try:
        #         return float(m.group(0).replace('MG','')), 'MG'
        #     except:
        #         print('ERROR: ', m.group(0).replace('MG',''), 'MG')
        #         return np.nan, 'MG'
        # m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.MG', str(string).upper())
        # if m != None:
        #     try:
        #         return float(m.group(0).replace('MG','')), 'MG'
        #     except:
        #         print('ERROR: ', m.group(0).replace('MG',''), 'MG')
        #         return np.nan, 'MG'

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+GL', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('GL',''))), 'GAL'
            except:
                print('ERROR: ', m.group(0).replace('GL',''), 'GAL')
                return np.nan, 'GAL' 
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.GL', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('GL',''))), 'GAL'
            except:
                print('ERROR: ', m.group(0).replace('GL',''), 'GAL')
                return np.nan, 'GAL' 

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+G', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('G',''))), 'GAL'
            except:
                print('ERROR: ', m.group(0).replace('G',''), 'GAL')
                return np.nan, 'GAL' 
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.G', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('G',''))), 'GAL'
            except:
                print('ERROR: ', m.group(0).replace('G',''), 'GAL')
                return np.nan, 'GAL' 

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+LITER', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('LITER',''))), 'L'
            except:
                print('ERROR: ', np.absolute(float(m.group(0).replace('LITER',''))), 'L')
                return np.nan, 'L' 
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.LITER', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('LITER',''))), 'L'
            except:
                print('ERROR: ', np.absolute(float(m.group(0).replace('LITER',''))), 'L')
                return np.nan, 'L' 
        
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+LTR', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('LTR',''))), 'L'
            except:
                print('ERROR: ', np.absolute(float(m.group(0).replace('LTR',''))), 'L')
                return np.nan, 'L' 
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.LTR', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('LTR',''))), 'L'
            except:
                print('ERROR: ', np.absolute(float(m.group(0).replace('LTR',''))), 'L')
                return np.nan, 'L' 

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+LIT', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('LIT',''))), 'L'
            except:
                print('ERROR: ', np.absolute(float(m.group(0).replace('LIT',''))), 'L')
                return np.nan, 'L' 
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.LIT', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('LIT',''))), 'L'
            except:
                print('ERROR: ', np.absolute(float(m.group(0).replace('LIT',''))), 'L')
                return np.nan, 'L' 
        
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+LT', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('LT',''))), 'L'
            except:
                print('ERROR: ', np.absolute(float(m.group(0).replace('LT',''))), 'L')
                return np.nan, 'L' 
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.LT', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('LT',''))), 'L'
            except:
                print('ERROR: ', np.absolute(float(m.group(0).replace('LT',''))), 'L')
                return np.nan, 'L' 

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+L', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('L',''))), 'L'
            except:
                print('ERROR: ', m.group(0).replace('L',''), 'L')
                return np.nan, 'L' 
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.L', str(string).upper())
        if m != None:
            try:
                return np.absolute(float(m.group(0).replace('L',''))), 'L'
            except:
                print('ERROR: ', m.group(0).replace('L',''), 'L')
                return np.nan, 'L' 
            

        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+KG', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('KG',''))), 'KG'
        m = re.search(r'([-+]?[0-9]*\.?[0-9]+)+.KG', str(string).upper())
        if m != None:
            return np.absolute(float(m.group(0).replace('KG',''))), 'KG'

        if '19.2' in str(string).upper():
            return 19.2, 'OZ'
        if '.375' in str(string).upper():
            return 375.0, 'ML'
        if '375' in str(string).upper():
            return 375.0, 'ML'
        if '750' in str(string).upper():
            return 750.0, 'ML'
        if '.750' in str(string).upper():
            return 750.0, 'ML'
        if '1.75' in str(string).upper():
            return 1.75, 'L'
        if '1.5' in str(string).upper():
            return 1.5, 'L'
        return np.nan, ''

    def regex_rule_container(self, string):
        string = ' ' + str(string).upper() + ' '
        m = re.search(r'PKCN|PK CN|OZCN|OZ CN|SGLCN|SGL CN|PKC|PK C|PC|OZC|OZ C|SGLC|CANS|CAN|CNS|CN', string)
        if m != None:
            return 'Cans'
        m = re.search(r'PKBTL|PK BTL|OZBTL|OZ BTL|SGLBTL|SGL BTL|ML BTL|MLBTL|PKB|PK B|OZB|OZ B|SGLB|BTL|BOTTLES|BOTTLE|ML B|MLB|BT|NR|LN', string)
        if m != None:
            return 'Bottles'
        # m = re.search(r' BOXED | BOX ', string)
        # if m != None:
        #     return 'Bag In Box'
        m = re.search(r' KEG|KEGS', string)
        if m != None:
            return 'Keg'
        m = re.search(r' JUG', string)
        if m != None:
            return 'Jug'
        return None
    
    #### Column Logic for applying Regex Logic above
    def _standardize_item_size(self, size, unit):
        if str(unit).upper() == 'L' and float(size) < 1.0:
            return size*1000.0, 'ML'
        elif str(unit).upper() == 'ML' and float(size) >= 1000.0:
            return size/1000.0, 'L'
        else:
            return size, unit
        
    def regex_logic_package_size(self, x):
        # if 'single' in str(x['rt_package_size']).lower() or 'single' in str(x['rt_brand_description']).lower() or 'single' in str(x['rt_item_size']).lower():
        #     return 1.0
        m = self.regex_rule_package_size(x['rt_package_size'])
        if not math.isnan(m):
            return m
        else:
            m = self.regex_rule_package_size(x['rt_brand_description'])
            if not math.isnan(m):
                return m
            else:
                return 1.0

    def regex_logic_item_size(self, x):
        size, unit = self.regex_rule_item_size(x['rt_item_size'])
        if not math.isnan(size):
            return self._standardize_item_size(size, unit)
        size, unit = self.regex_rule_item_size(x['rt_package_size'])
        if not math.isnan(size):
            return self._standardize_item_size(size, unit)
        else:
            size, unit = self.regex_rule_item_size(x['rt_brand_description'])
            return self._standardize_item_size(size, unit)

    def regex_logic_container(self, x):
        m = self.regex_rule_container(x['rt_item_size'])
        if isinstance(m, str):
            return m
        m = self.regex_rule_container(x['rt_package_size'])
        if isinstance(m, str):
            return m
        else:
            return self.regex_rule_container(x['rt_brand_description'])
        
    

    #### Larger functions for calling all above (these functions actually do stuff)
    # Handle special package types
    def parse_special_package_configurations(self, x):
        if x['item_units'] == 'BOMBER':
            return  [22,'OZ','Bottles']
        if x['item_units'] == 'PINT':
            return [16,'OZ', x['container_type']]
        else:
            return [x['item_size_value'],x['item_units'], x['container_type']]
    
    # Handle filling when container_type is missing using item_size to create logic on assumed container type
    def fill_missing_container(self, x):
        if not x['container_type']:
            if x['item_size_value'] >= 500 and x['item_units'] == 'ML' :
                return 'Bottles'
            elif x['item_units'] == 'L' and x['item_size_value'] < 3:
                return 'Bottles'
            elif x['item_units'] == 'LB' :
                return 'Bag'
            else:
                return ''
        else:
            return x['container_type'] 
            
    
    def parse_configuration(self, df):
        df['package_size_num'] = df.apply(lambda x: self.regex_logic_package_size(x), axis=1)
        df[['item_size_value','item_units']] = df.apply(lambda x: self.regex_logic_item_size(x), axis=1, result_type="expand")
        df['container_type'] = df.apply(lambda x: self.regex_logic_container(x), axis=1)
        
        # Special Package names (i.e. bomber and pint)
        df[['item_size_value','item_units','container_type']] = df.apply(lambda x:  self.parse_special_package_configurations(x), result_type='expand',axis=1)
        
        # Create item_size col
        df['item_size'] = df['item_size_value'].astype(str) + ' ' + df['item_units'].astype(str)
        
        # Clean up item_size parsing
        df['item_size'] = df['item_size'].str.replace('.0 ',' ')
        df['item_size'] = df['item_size'].str.replace('nan','')
        
        # If missing container type for larger items make bottle
        #### maybe add a function for handling this
        df['container_type'] = df.apply(self.fill_missing_container, axis=1)
        
        ### Need to add something to do with bottle types? Like PET or Tetra or Pouch?
        return df
        
    def _check_empty_string(self, s):
        s = str(s)
        if not s:
            return True
        elif s.isspace():
            return True
        else:
            return False
        
    def build_package_configuration(self, x):
        if self._check_empty_string(x['item_size']) and self._check_empty_string(x['container_type']):
            return str(int(x['package_size_num'])) + ' pack'
        
        elif self._check_empty_string(x['container_type']):
             return str(int(x['package_size_num'])) + ' pack ' + str(x['item_size'])
            
        elif self._check_empty_string(x['item_size']):
             return str(int(x['package_size_num'])) + ' pack ' + str(x['container_type'])

        else:
            return str(int(x['package_size_num'])) + ' pack ' + str(x['item_size']) + ' ' + str(x['container_type'])
        
       
        
    #### tie it all together into a single function
    def main(self, df):
        df = self.parse_configuration(df)
        df['package_configuration'] = df.apply(self.build_package_configuration, axis=1)

#         if product_type.upper() == 'WINE' or product_type.upper() == 'LIQUOR':
#             matches_df['container_type'] = matches_df['container_type'].fillna('B')
            # print(matches_df['container_type'].value_counts(dropna=False))
#         if product_type.upper() == 'BEER':
#             matches_df['item_size_value'] = matches_df['item_size_value'].fillna(12)
#             matches_df['item_units'] = matches_df['item_units'].fillna('OZ')

        return df