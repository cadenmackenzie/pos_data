import os
from glob import glob
import sys
import unittest
import pandas as pd
from  pandas.testing import assert_frame_equal

module_path = os.path.abspath(os.path.join('../src'))
if module_path not in sys.path:
    sys.path.append(module_path)
from data_processing_lambda import processMPower, processTiger, processAdvent, processLiquorPos

class TestDataProcessing(unittest.TestCase):

    # def __init__(self):
    #     pass

    def test_load_data_exceptions(self):
        '''
        Raise proper exceptions for processMPower.load_data()
        '''
        # If filename is not csv file
        pos = processMPower()
        with self.assertRaises(Exception) as context:
            pos.load_data(input_filenames ='test_1.xlsx')
            self.assertTrue("Unrecognized file type - expecting .csv extension." in str(context.exception))
        pos = processTiger()
        with self.assertRaises(Exception) as context:
            pos.load_data(input_filenames ='test_1.xlsx')
            self.assertTrue("Unrecognized file type - expecting .csv extension." in str(context.exception))
        pos = processAdvent()
        with self.assertRaises(Exception) as context:
            pos.load_data(input_filenames ='test_1.xlsx')
            self.assertTrue("Unrecognized file type - expecting .csv extension." in str(context.exception))
        pass

    def test_load_csv(self):
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
        test.to_csv('test_pipe.csv', index=False, sep='|')

        pos = processMPower()
        df = pos.load_data(input_filenames ='test.csv')
        assert_frame_equal(df, test)

        pos = processTiger()
        df = pos.load_data(input_filenames ='test_pipe.csv')
        assert_frame_equal(df, test)

        pos = processAdvent()
        df = pos.load_data(input_filenames ='test_pipe.csv')
        assert_frame_equal(df, test)

        os.remove('test.csv')
        os.remove('test_pipe.csv')
        pass

    def test_load_dbf(self):
        '''
        Load dbf if conditions met
        '''

        pos = processLiquorPos()
        df = pos.load_data(input_filenames ='easthoustonstwineandliquor_handoff.zip')

        if 'CODE_NUM' in df.columns.tolist():
            x = True
        else:
            x = False
        self.assertTrue(x)

        if 'BARCODE' in df.columns.tolist():
            x = True
        else:
            x = False
        self.assertTrue(x)

        # Clean up
        try:
            for x in glob('__MACOSX/easthoustonstwineandliquor_handoff/*'):
                os.remove(x)
            os.rmdir('__MACOSX/easthoustonstwineandliquor_handoff')
            os.rmdir('__MACOSX')
        except:
            print("Can't find __MACOSX/easthoustonstwineandliquor_handoff dir")
        
        for x in glob('easthoustonstwineandliquor_handoff/*'):
            os.remove(x) 
        os.rmdir('easthoustonstwineandliquor_handoff')
        pass

    def test_mpower_process_data(self):
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

        pos = processMPower()
        df = pos.load_data(input_filenames ='test.csv')
        df = pos.process_data(df)

        self.assertEqual(df.columns.tolist(), ['rt_product_id','rt_upc_code','rt_brand_name',
                                        'rt_brand_description','rt_product_type','rt_product_category',
                                        'rt_package_size','rt_item_size','price_regular',
                                        'price_sale','qty_on_hand'
                                        ])
        self.assertCountEqual(df['price_sale'], [0,0,0])
        os.remove('test.csv')
        pass

    def test_liquorpos_process_data(self):
        '''
        test processing of liquorPos
        '''
        pos = processLiquorPos()
        df = pos.load_data(input_filenames ='easthoustonstwineandliquor_handoff.zip')
        df = pos.process_data(df)

        print(df)

        self.assertEqual(df.columns.tolist(), ['rt_product_id','rt_upc_code','rt_brand_name',
                                        'rt_brand_description','rt_product_type','rt_product_category',
                                        'rt_package_size','rt_item_size','price_regular',
                                        'price_sale','qty_on_hand'
                                        ])
        
        # Clean up
        try:
            for x in glob('__MACOSX/easthoustonstwineandliquor_handoff/*'):
                os.remove(x)
            os.rmdir('__MACOSX/easthoustonstwineandliquor_handoff')
            os.rmdir('__MACOSX')
        except:
            print("Can't find __MACOSX/easthoustonstwineandliquor_handoff dir")
        
        for x in glob('easthoustonstwineandliquor_handoff/*'):
            os.remove(x) 
        os.rmdir('easthoustonstwineandliquor_handoff')
        pass

if __name__ == '__main__':
    unittest.main()