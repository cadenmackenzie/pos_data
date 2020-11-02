import os
import sys
import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal

module_path = os.path.abspath(os.path.join('../'))
if module_path not in sys.path:
    sys.path.append(module_path)
from data_processing_lambda import processPOSNAME1Pos

class TestDataProcessing(unittest.TestCase):

    # def __init__(self):
    #     pass

    def test_pos_1_load_data_exceptions(self):
        '''
        Raise proper exceptions for processPOSNAME1Pos.load_data()
        '''
        # If filename not a list
        pos = processPOSNAME1Pos()
        with self.assertRaises(Exception) as context:
            pos.load_data(input_filenames ='test.csv')
            self.assertTrue("input_filenames is not a list - expecting input_filenames to be a list with single .csv file." in str(context.exception))

        # If filename is multiple item list
        pos = processPOSNAME1Pos()
        with self.assertRaises(Exception) as context:
            pos.load_data(input_filenames =['test_1.csv','test_2.csv'])
            self.assertTrue("More than one file passed - expecting a single .csv file in input_filenames." in str(context.exception))

        # If filename is not csv file
        pos = processPOSNAME1Pos()
        with self.assertRaises(Exception) as context:
            pos.load_data(input_filenames =['test_1.xlsx'])
            self.assertTrue("Unrecognized file type - expecting .csv extension." in str(context.exception))
        pass

    def test_pos_1_load_csv(self):
        '''
        Load csv if conditions met
        '''
        test = pd.DataFrame(data={'brand_name':['Bud Light','Oskar Blues','Coors'],
                                'brand_description':['Bud Light 6 pk', 'Oskar Blues Dales Pale Ale 12 pk','Coors Light 24 pk'],
                                'price_regular':[9.99, 13.99,24.99],
                                'product_id':[1000,1001,1002],
                                'upc_code':[12340,56780,11110],
                                'product_type':['BEER','BEER','BEER'],
                                'product_category':['light beer','american pale ale','light beer'],
                                'package_size':['6pk','12 pk','24pk'],
                                'qty_on_hand':[20,0,34]
                                })
        test.to_csv('test.csv', index=False)

        pos = processPOSNAME1Pos()
        df = pos.load_data(input_filenames =['test.csv'])
        assert_frame_equal(df, test)

        os.remove('test.csv')
        pass

    def test_pos_1_process_data(self):
        '''
        Process csv
        '''
        test = pd.DataFrame(data={'brand_name':['Bud Light','Oskar Blues','Coors'],
                                'brand_description':['Bud Light 6 pk', 'Oskar Blues Dales Pale Ale 12 pk','Coors Light 24 pk'],
                                'price_regular':[9.99, 13.99,24.99],
                                'product_id':[1000,1001,1002],
                                'upc_code':[12340,56780,11110],
                                'product_type':['BEER','BEER','BEER'],
                                'product_category':['light beer','american pale ale','light beer'],
                                'package_size':['6pk','12 pk','24pk'],
                                'qty_on_hand':[20,0,34]
                                })
        test.to_csv('test.csv', index=False)

        pos = processPOSNAME1Pos()
        df = pos.load_data(input_filenames =['test.csv'])
        df = pos.process_data(df)

        self.assertEqual(df.columns.tolist(), ['rt_product_id','rt_upc_code','rt_brand_name',
                                        'rt_brand_description','rt_product_type','rt_product_category',
                                        'rt_package_size','rt_item_size','price_regular',
                                        'price_sale','qty_on_hand'
                                        ])
        self.assertEqual(df['price_sale'], [0,0,0])

if __name__ == '__main__':
    unittest.main()