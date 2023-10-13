"""@file test_class_installbase.py.

@brief This file used to test code for below M2M Data:
    -Shipment Data
    -Serial Data
    -BOM Data





@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# Pytest execution command
#!pytest ./tests/test_class_installbase.py
#!pytest --cov
#!pytest --cov=.\src --cov-report html:.\coverage\ .\test\
#!pytest --cov=.\src\class_ProSQL.py --cov-report html:.\coverage\ .\test\

#%%
from datetime import datetime, date, timedelta

import numpy as np
import pytest
import pandas as pd
from pandas._testing import assert_frame_equal

from utils.filter_data import Filter
from utils.logger import AppLogger
from utils.dcpd import InstallBase
from utils.transform import Transform
logger = AppLogger('DCPD', level='')
filters_ = Filter()
from utils.dcpd.class_business_logic import BusinessLogic
obj_bus_logic = BusinessLogic()
obj_install_base = InstallBase()
import numpy
#%%

class TestPrioratizedColumn:
    """
    Test the priority of columns if it contains None value
    """

    @pytest.mark.parametrize(
        "df_data_install",
        [None,
         dict(),
         [],
         (pd.DataFrame()),
         (pd.DataFrame(data={'ShipTo_Country': ['usa', 'usa', None, 'usa',
                                                None, 'united states']})),
         (pd.DataFrame(data={'SoldTo_Country': ['canada', None, 'usa', None,
                                                'usa', 'united states']})),
         ], )
    def test_prioratized_columns_errors(self, df_data_install):
        """
            Provided "df_data_install" with
            None DataFrame
            Empty DataFrame
            Missing Columns "ShipTo_Country"
            Missing Columns "SoldTo_Country", should throw errors
        """
        ls_priority = ['ShipTo_Country', 'SoldTo_Country']
        with pytest.raises(Exception) as _:
            filters_.prioratized_columns(df_data_install, ls_priority)

    def test_prioratized_columns_ideal_scenario(self):
        """
        Provided 3 columns with None value, to check priority
        """
        df_data_install = pd.DataFrame(data={
            'ShipTo_Country': ['usa', None, None, 'usa', None, None],
            'SoldTo_Country': ['canada', None, 'usa', None, 'canada', None],
            'buyer_country': [None, 'usa', 'canada', 'canada', 'usa', None]})
        exp_res = np.array(pd.Series(data=['usa', 'usa', 'usa', 'usa',
                                           'canada', None]))
        ls_priority = ['ShipTo_Country', 'SoldTo_Country', 'buyer_country']
        res = filters_.prioratized_columns(
            temp_data_org =  df_data_install,
            ls_priority = ls_priority)
        assert res.all() == exp_res.all()


class TestFilterData:
    """
    Test data filtered properly as configuration filters
    """

    @pytest.mark.parametrize(
        "df_filter_data_install",
        [None,
         dict(),
         [],
         (pd.DataFrame()),
         (pd.DataFrame(data={
             'InstallSize': [0, 1, 1]
         }))
         ], )
    def test_filter_data_errors_1(self, df_filter_data_install):
        """
        Provided "df_filter_data_install" with
        None DataFrame
        Empty DataFrame
        Missing Columns in DataFrame all columns except one, should throw error
        """
        config_filter = obj_install_base.config['database']['M2M']['Filters']
        with pytest.raises(Exception) as _:
            filters_.filter_data(df_filter_data_install, config_filter)


    @pytest.mark.parametrize(
        "df_filter_data_install",
        [(pd.DataFrame(
            data={
                'ShipmentDate': ['9/20/1984  12:00:00 AM',
                                 '9/20/2028  12:00:00 AM', '9/20/2014  12:00:00 PM'],
                'ProductClass': ['70', '71', '76'],
                'Country': ['usa', 'United States of America',
                            'us'],
                'PartNumber_TLN_Shipment': ['ckb',
                                            'seismic calculations', 'chs']
            })),
            (pd.DataFrame(
                data={
                    'ShipmentDate': ['9/20/1994  12:00:00 AM',
                                     '9/20/2030  12:00:00 AM', '9/20/2012  12:00:00 PM'],
                    'InstallSize': [0, 1, 1],
                    'Country': ['usa', 'us',
                                'United States of America'],
                    'PartNumber_TLN_Shipment': ['ckb',
                                                'seismic calculations', 'chs']
                })),
            (pd.DataFrame(
                data={
                    'ShipmentDate': ['9/20/1974  12:00:00 AM',
                                     '9/20/2004  12:00:00 AM', '9/20/2024  12:00:00 PM'],
                    'InstallSize': [0, 1, 1],
                    'ProductClass': ['70', '73', '76'],
                    'PartNumber_TLN_Shipment': ['ckb',
                                                'seismic calculations', 'chs']
                })),
            (pd.DataFrame(
                data={
                    'ShipmentDate': ['9/20/1974  12:00:00 AM',
                                     '9/20/2004  12:00:00 AM', '9/20/2024  12:00:00 PM'],
                    'InstallSize': [0, 1, 1],
                    'ProductClass': ['70', '73', '76'],
                    'Country': ['usa', 'us',
                                'United States of America']
                }))
        ])
    def test_filter_data_errors_2(self, df_filter_data_install):
        """
        Provided "df_filter_data_install" with
        Missing Columns "InstallSize"
        Missing Columns "ProductClass"
        Missing Columns "Country"
        Missing Columns "PartNumber_TLN_Shipment", should throw error
        """
        df_filter_data_install['ShipmentDate'] = pd.to_datetime(
            df_filter_data_install['ShipmentDate'])
        config_filter = obj_install_base.config['database']['M2M']['Filters']
        with pytest.raises(Exception) as _:
            filters_.filter_data(df_filter_data_install, config_filter)

    def test_filter_data_ideal_scenario_1(self):
        """
        Provided invalid "ShipmentDate" which should be filtered
        """
        df_filter_data_install = pd.DataFrame(
            data={
                'ShipmentDate': ['9/20/2011  12:00:00 AM',
                                 '9/20/1914  12:00:00 AM', '01/01/1995  12:00:00 AM',
                                 '9/20/2024  12:00:00 PM',
                                 datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
                                 date.today() + timedelta(days=10)],
                'InstallSize': [1, 2, 1, 5, 9, 1],
                'ProductClass': ['10', '21', '1g', '8B', 'MO', '15'],
                'Country': ['usa', 'United States of America',
                            'us', 'usa', 'United States of America', 'us'],
                'PartNumber_TLN_Shipment': ['qwer', 'asdf', 'xyz', 'zxcv', 'plm', 'trfsk']
            })
        exp_res = pd.DataFrame(data={'f_all': [True, False, True, False, True, False]})
        df_filter_data_install['ShipmentDate'] = pd.to_datetime(
            df_filter_data_install['ShipmentDate'])
        config_filter = obj_install_base.config['database']['M2M']['Filters']
        res = filters_.filter_data(df_filter_data_install, config_filter)

        assert exp_res['f_all'].equals(res['f_all'])

    def test_filter_data_ideal_scenario_2(self):
        """
        Provided invalid "InstallSize" which should be filtered
        """
        df_filter_data_install = pd.DataFrame(
            data={
                'ShipmentDate': ['9/20/2011  12:00:00 AM',
                                 '9/20/2004  12:00:00 AM', '9/20/1995  12:00:00 AM',
                                 '9/20/2014  12:00:00 PM',
                                 datetime.today().strftime('%Y-%m-%d %H:%M:%S')],
                'InstallSize': [1, 2, -1, None, 0],
                'ProductClass': ['10', '21', '1g', '8B', 'MO'],
                'Country': ['usa', 'United States of America',
                            'us', 'usa', 'United States of America'],
                'PartNumber_TLN_Shipment': ['qwer', 'asdf', 'xyz', 'zxcv', 'plm']
            })
        exp_res = pd.DataFrame(data={'f_all': [True, True, False, False, False]})
        df_filter_data_install['ShipmentDate'] = pd.to_datetime(
            df_filter_data_install['ShipmentDate'])
        config_filter = obj_install_base.config['database']['M2M']['Filters']
        res = filters_.filter_data(df_filter_data_install, config_filter)

        assert exp_res['f_all'].equals(res['f_all'])

    def test_filter_data_ideal_scenario_3(self):
        """
        Provided invalid "ProductClass" which should be filtered
        """
        df_filter_data_install = pd.DataFrame(
            data={
                'ShipmentDate': ['9/20/2011  12:00:00 AM',
                                 '9/20/2004  12:00:00 AM', '9/20/1995  12:00:00 AM',
                                 '9/20/2014  12:00:00 PM',
                                 datetime.today().strftime('%Y-%m-%d %H:%M:%S')],
                'InstallSize': [1, 2, 1, 5, 10],
                'ProductClass': ['77', '21', '71', '8B', '79'],
                'Country': ['usa', 'United States of America',
                            'us', 'usa', 'United States of America'],
                'PartNumber_TLN_Shipment': ['qwer', 'asdf', 'xyz', 'zxcv', 'plm']
            })
        exp_res = pd.DataFrame(data={'f_all': [False, True, False, True, False]})
        df_filter_data_install['ShipmentDate'] = pd.to_datetime(
            df_filter_data_install['ShipmentDate'])
        config_filter = obj_install_base.config['database']['M2M']['Filters']
        res = filters_.filter_data(df_filter_data_install, config_filter)

        assert exp_res['f_all'].equals(res['f_all'])

    def test_filter_data_ideal_scenario_4(self):
        """
        Provided invalid "Country" which should be filtered
        """
        df_filter_data_install = pd.DataFrame(
            data={'ShipmentDate': ['9/20/2011  12:00:00 AM',
                                   '9/20/2004  12:00:00 AM', '9/20/1995  12:00:00 AM',
                                   '9/20/2014  12:00:00 PM',
                                   datetime.today().strftime('%Y-%m-%d %H:%M:%S')],
                  'InstallSize': [1, 2, 1, 5, 10],
                  'ProductClass': ['17', '21', '10', '8B', 'MO'],
                  'Country': ['usa', 'india',
                              'us', 'canada', 'United States of America'],
                  'PartNumber_TLN_Shipment': ['qwer', 'asdf', 'xyz', 'zxcv', 'plm']
                  })
        exp_res = pd.DataFrame(data={'flag_Country': [True,
                                                      False, True,
                                                      False, True]})
        df_filter_data_install['ShipmentDate'] = pd.to_datetime(
            df_filter_data_install['ShipmentDate'])
        config_filter = obj_install_base.config['database']['M2M']['Filters']
        res = filters_.filter_data(df_filter_data_install, config_filter)

        assert exp_res['flag_Country'].equals(res['flag_Country'])

    def test_filter_data_ideal_scenario_5(self):
        """
        Provided invalid "PartNumber_TLN_Shipment" which should be filtered
        """
        df_filter_data_install = pd.DataFrame(
            data={
                'ShipmentDate': ['9/20/2017  12:00:00 AM',
                                 '9/20/2021  12:00:00 AM', '9/20/2011  12:00:00 AM',
                                 '9/20/2004  12:00:00 AM', '9/20/1995  12:00:00 AM',
                                 '9/20/2014  12:00:00 PM',
                                 datetime.today().strftime('%Y-%m-%d %H:%M:%S')],
                'InstallSize': [1, 2, 1, 5, 10, 43, 55],
                'ProductClass': ['17', '21', '10', '8B', 'MO', '1g', 'CN'],
                'Country': ['usa', 'United States of America',
                            'us', 'usa', 'United States of America',
                            'usa', 'us'],
                'PartNumber_TLN_Shipment': ['factory witness Testing',
                                            'seismic calculations', 'xyz',
                                            'Seismic calculations',
                                            'mod', 'chs', 'ckb']
            })
        exp_res = pd.DataFrame(data={'f_all': [False, False, True,
                                               False, False, False, False]})
        df_filter_data_install['ShipmentDate'] = pd.to_datetime(
            df_filter_data_install['ShipmentDate'])
        config_filter = obj_install_base.config['database']['M2M']['Filters']
        res = filters_.filter_data(df_filter_data_install, config_filter)

        assert exp_res['f_all'].equals(res['f_all'])

    @pytest.mark.parametrize(
        "df_install",
        [None,
         {},
         [],
         (pd.DataFrame())
         ], )
    def test_filter_data_final_error(self, df_install):
        """
        Provided "df_install" with
        None DataFrame
        Empty DataFrame
        Missing Columns in DataFrame all columns except one, should throw error
        """
        with pytest.raises(Exception) as _:
            print(obj_install_base.filter_mtmdata(df_install))

    @pytest.mark.parametrize(
        "df_install",
        [None,
         (pd.DataFrame()),
         (pd.DataFrame(data={'Product_M2M': ['', 'abc', 'mno','rew']})),
         (pd.DataFrame(data={'SOStatus': ['cancelled', 'open', 'closed','closed']})),
         (pd.DataFrame(data={'is_in_usa': [True, False, False, False]}))
         ])
    def test_filter_data_mtm_ideal_data(self, df_install):
        """
        Provided "df_install" with
        Empty DataFrame
        Ideal data to filter out the results from the final data

        """
        with pytest.raises(Exception) as _:
            obj_install_base.filter_mtmdata(df_install)

class TestFilterProductClass:
    """
    Test data filtered for Product class
    """

    @pytest.mark.parametrize(
        "ref_prod",
        [None,
         [],
         {},
         (pd.DataFrame()),
         (pd.DataFrame(data={'product_type': ['asd', 'asda', 'dass'],
                             'product_prodclass': ['PDU', 'Trans', 'RPP']})),
         (pd.DataFrame(data={'ProductClass': ['11', '43', '12'],
                             'product_type': ['asd', 'asda', 'dass']})),
         (pd.DataFrame(data={'ProductClass': ['11', '43', '12'],
                             'product_prodclass': ['PDU', 'Trans', 'RPP']}))
         ],
    )
    def test_filter_product_class_errors_1(self, ref_prod):
        """
        Provided "ref_prod" with
        None DataFrame
        Empty DataFrame
        Missing Columns "ProductClass"
        Missing Columns "product_prodclass"
        Missing Columns "product_type", should throw errors
        """
        df_data_install = pd.DataFrame(data={'ShipmentDate': ['9/20/1994',
                                                              '9/20/2004', '9/20/2014'],
                                             'InstallSize': [0, 1, 0],
                                             'ProductClass': ['11', '71', '43'],
                                             'SoldTo_Country': ['usa', 'usa',
                                                                'usa'],
                                             })
        ls_cols = df_data_install.columns.tolist()
        with pytest.raises(Exception) as _:
            obj_install_base.filter_product_class(
                ref_prod, df_data_install, ls_cols)

    @pytest.mark.parametrize(
        "df_data_install",
        [None,
         [],
         {},
         (pd.DataFrame()),
         (pd.DataFrame(data={'ShipmentDate': ['9/20/1994', '9/20/2004',
                                              '9/20/2014'],
                             'InstallSize': [0, 1, 0],
                             'SoldTo_Country': ['usa', 'usa', 'usa'],
                             }))],
    )
    def test_filter_product_class_errors_2(self, df_data_install):
        """
        Provided "df_data_install" with
        None DataFrame
        Empty DataFrame
        Missing Columns "ProductClass", should throw errors
        """
        ref_prod = pd.DataFrame(data={'ProductClass': ['11', '43', '12'],
                                      'product_type': ['asd', 'asda', 'dass'],
                                      'product_prodclass': ['PDU', 'Trans', 'RPP']})
        ls_cols = ['ShipmentDate', 'InstallSize', 'ProductClass', 'SoldTo_Country']
        with pytest.raises(Exception) as _:
            obj_install_base.filter_product_class(ref_prod, df_data_install, ls_cols)

    def test_filter_product_class_ideal_scenario(self):
        """
        Data should be merged on ProductClass and invalid "product_prodclass" should be filtered
        """
        ref_prod = pd.DataFrame(data={'ProductClass': ['11', '43', '12', '21', '53'],
                                      'product_type': ['asd', 'asda', 'dass',
                                                       'wedrf', 'pkom'],
                                      'product_prodclass': ['PDU', 'sts',
                                                            'XYZ', 'abc', 'rpp']})
        df_data_install = pd.DataFrame(
            data={'ShipmentDate': ['3/10/1995', '2/27/2004', '9/20/2014',
                                   '9/20/2023', '9/20/2023'],
                  'InstallSize': [10, 1, 20, 65, 15],
                  'ProductClass': ['11', '43', '17', '21', '35'],
                  'SoldTo_Country': ['usa', 'us', 'usa',
                                     'United States of America', 'us'],
                  })
        exp_res = pd.DataFrame(
            data={'ShipmentDate': ['3/10/1995', '2/27/2004'],
                  'InstallSize': [10, 1], 'ProductClass': ['11', '43'],
                  'SoldTo_Country': ['usa', 'us'],
                  'product_type': ['asd', 'asda'],
                  'product_prodclass': ['pdu', 'sts']})
        ls_cols = df_data_install.columns.tolist()
        res, _ = obj_install_base.filter_product_class(
            ref_prod, df_data_install, ls_cols)

        assert exp_res.equals(res)


class TestFilterKeySerial:
    """
        Test foreign key generation and removal of duplicate values
    """

    @pytest.mark.parametrize(
        "df_data_install",
        [None,
         [],
         {},
         (pd.DataFrame()),
         (pd.DataFrame(data={'Shipper_Index': [123, 121, 342, 321],
                             'ShipperItem_Index': [1, 1, 2, 3],
                             'Job_Index': ['0129-0000', '0159-0000',
                                           '0429-0000', '0129-0000'],
                             'SO': [220, 754, 971, 332]
                             })),
         (pd.DataFrame(data={'f_all': [True, True, True, False],
                             'ShipperItem_Index': [1, 1, 2, 3],
                             'Job_Index': ['0129-0000', '0159-0000',
                                           '0429-0000', '0129-0000'],
                             'SO': [220, 754, 971, 332]
                             })),
         (pd.DataFrame(data={'f_all': [True, True, True, False],
                             'Shipper_Index': [123, 121, 342, 321],
                             'Job_Index': ['0129-0000', '0159-0000',
                                           '0429-0000', '0129-0000'],
                             'SO': [220, 754, 971, 332]
                             })),
         (pd.DataFrame(data={'f_all': [True, True, True, False],
                             'Shipper_Index': [123, 121, 342, 321],
                             'ShipperItem_Index': [1, 1, 2, 3],
                             'Job_Index': ['0129-0000', '0159-0000',
                                           '0429-0000', '0129-0000']
                             }))
         ])
    def test_filter_key_serial_errors_1(self, df_data_install):
        """
        Provided "df_data_install" with
        None DataFrame
        Empty DataFrame
        Missing Columns "f_all"
        Missing Columns "Shipper_Index"
        Missing Columns "ShipperItem_Index"
        Missing Columns "Job_Index"
        Missing Columns "SO", should throw errors
        """
        ls_cols = ['f_all', 'Shipper_Index', 'ShipperItem_Index',
                   'Job_Index', 'SO']
        with pytest.raises(Exception) as _:
            obj_install_base.filter_key_serial(df_data_install, ls_cols)

    def test_filter_key_serial_ideal_scenario(self):
        """
        Provided duplicate data and False value to f_all to check filter
        """
        df_data_install = pd.DataFrame(data={'f_all': [True, True, True, False],
                                             'Shipper_Index': [121, 342, 121, 321],
                                             'ShipperItem_Index': [1, 2, 1, 3],
                                             'Job_Index': ['0129-0000',
                                                           '0429-0000', '0159-0000', '0129-0000'],
                                             'SO': [220, 971, 754, 332]
                                             })

        ls_cols = df_data_install.columns.tolist()
        exp_res = pd.DataFrame(data={'f_all': [True, True],
                                     'Shipper_Index': [121, 342],
                                     'ShipperItem_Index': [1, 2],
                                     'Job_Index': ['0129-0000', '0429-0000'],
                                     'SO': [220, 971],
                                     'key_serial': ['121:1', '342:2'],
                                     'key_bom': ['0129-0000', '0429-0000'],
                                     'key_serial_shapepoint': ['220', '971']
                                     })
        res = obj_install_base.filter_key_serial(df_data_install, ls_cols)

        assert exp_res.equals(res)


class TestCombineSerialnumData:
    """
            Test serial number combined well after process
    """

    @pytest.mark.parametrize(
        "df_srnum_range",
        [None,
         [],
         {},
         (pd.DataFrame()),
         (pd.DataFrame(data={'key_serial': ['123:1', '121:1', '342:2'],
                             'Product': ['xyz', 'adx', 'erw']})),
         ])
    def test_combine_serialnum_data_errors_1(self, df_srnum_range):
        """
        Provided "df_srnum_range" with
        None DataFrame
        Empty DataFrame
        Missing Columns "Shipper_Index", should throw errors
        """
        df_out = pd.DataFrame(data={'SerialNumberOrg': [12, 34, 26],
                                    'key_serial': ['123:1', '121:1', '342:2'],
                                    'SerialNumber': [12, 34, 26],
                                    'Product': ['xyz', 'adx', 'erw']})
        df_data_install = pd.DataFrame(
            data={'key_serial': ['123:1', '121:1', '342:2'],
                  'Shipper_Index': [123, 121, 342],
                  'Shipper_Qty': [1, 1, 2],
                  'SerialNumber': [12, 34, 26],
                  'Product': ['xyz', 'adx', 'erw']})
        df_couldnot = pd.DataFrame(data={'key_serial': ['123:1', '121:1', '342:2'],
                                         'Shipper_Index': [123, 121, 342]})
        df_srnum = pd.DataFrame(
            data={'key_serial': [123, 121, 654],
                  'Shipper_Index': [123, 342, 321],
                  'Shipper_Qty': [1, 1, 2],
                  'SerialNumber': [12, 34, 26],
                  'Product': ['xyz', 'adx', 'erw']})
        merge_type = 'inner'
        with pytest.raises(Exception) as _:
            obj_install_base.combine_serialnum_data(df_srnum_range,
                                                    df_srnum, df_data_install,
                                                    df_out, df_couldnot,
                                                    merge_type)

    def test_combine_serialnum_data_ideal_scenario(self):
        """
        Test merging of serial number data with original data
        """
        df_srnum_range = pd.DataFrame(data={'SerialNumber': ['123-As1-3-4',
                                                             '181-547-7-8',
                                                             '835-063-12-14'],
                                            'key_serial': ['123:1', '121:1',
                                                           '342:2'],
                                            'Product': ['xyz', 'adx', 'erw']})
        df_srnum = pd.DataFrame(
            data={'key_serial': ['123:1', '121:1', '342:2'],
                  'Shipper_Index': [123, 342, 321],
                  'Shipper_Qty': [1, 1, 2],
                  'SerialNumber': ['12', '34', '26'],
                  'Product': ['xyz', 'adx', 'erw']})
        df_out = pd.DataFrame(data={'SerialNumberOrg': ['123-As1-3-4',
                                                        '181-547-7-8',
                                                        '835-063-12-14'],
                                    'InstallSize': [0, 1, 1],
                                    'SerialNumber': ['123-As1', '181-547',
                                                     '835-063'],
                                    'KeySerial': ['123:1', '121:1',
                                                   '342:2']
                                    })
        df_data_install = pd.DataFrame(
            data={'key_serial': ['123:1', '121:1', '342:2'],
                  'Shipper_Index': [123, 121, 342],
                  'Shipper_Qty': [1, 1, 2],
                  'SerialNumber': ['123-As1-3-4', '181-547-7-8', '835-063-12-14'],
                  'Product': ['xyz', 'adx', 'erw']})
        df_couldnot = pd.DataFrame(data={'key_serial': ['123:1', '121:1', '342:2'],
                                         'Shipper_Index': [123, 121, 342]})
        merge_type = 'inner'
        exp_res = pd.DataFrame(
            data={'key_serial': ['123:1', '123:1', '121:1', '121:1', '342:2'],
                  'Shipper_Index': [123, 123, 121, 121, 342],
                  'Shipper_Qty': [1, 1, 1, 1, 2],
                  'SerialNumber': ['123-As1-3-4', '123-As1-3-4',
                                   '181-547-7-8', '181-547-7-8', '835-063-12-14'],
                  'Product': ['xyz', 'xyz', 'adx', 'adx', 'erw'],
                  'SerialNumber_M2M': ['12', '123-As1', '34', '181-547', '835-063'],
                  'Product_M2M': ['xyz', 'xyz', 'adx', 'adx', 'erw']})
        res = obj_install_base.combine_serialnum_data(df_srnum_range,
                                                      df_srnum, df_data_install,
                                                      df_out, df_couldnot,
                                                      merge_type)

        assert exp_res.equals(res)


class TestPreprocessExpandRange:
    """
        Test preprocess before expanding serial number data
    """

    @pytest.mark.parametrize(
        "df_data_install",
        [None,
         {},
         [],
         (pd.DataFrame()),
         (pd.DataFrame(data={'key_serial': [123, 121, 342, 321]})),
         (pd.DataFrame(data={'Shipper_Qty': [1, 1, 2, 3]}))
         ])
    def test_preprocess_expand_range_errors_1(self, df_data_install):
        """
        Provided "df_data_install" with
        None DataFrame
        Empty DataFrame
        Missing Columns "Shipper_Qty"
        Missing Columns "key_serial", should throw errors
        """
        df_srnum = pd.DataFrame(data={'key_serial': [123, 121, 342, 321]})
        with pytest.raises(Exception) as _:
            obj_install_base.preprocess_expand_range(df_data_install, df_srnum)

    @pytest.mark.parametrize(
        "df_srnum",
        [None,
         [],
         {},
         (pd.DataFrame())
         ])
    def test_preprocess_expand_range_errors_2(self, df_srnum):
        """
        Provided "df_srnum" with
        None DataFrame
        Empty DataFrame, should throw errors
        """
        df_data_install = pd.DataFrame(data={'key_serial': [123, 121, 342, 321],
                                             'Shipper_Qty': [1, 0, 2, 3]})
        with pytest.raises(Exception) as _:
            obj_install_base.preprocess_expand_range(df_data_install, df_srnum)

    def test_preprocess_expand_range_ideal_scenario(self):
        """
        Test if Shipper_Qty < 1 then data will be filtered out
        """
        df_data_install = pd.DataFrame(data={'key_serial': [123, 121, 342],
                                             'Shipper_Qty': [2, 0, -1],
                                             'InstallSize': [0, 1, 0],
                                             'ProductClass': ['11', '71', '43']})
        df_srnum = pd.DataFrame(data={'key_serial': [123, 121, 654],
                                      'Shipper_Index': [123, 342, 321]
                                      })
        exp_res = pd.DataFrame(data={'key_serial': [123],
                                     'Shipper_Index': [123],
                                     'Shipper_Qty': [2],
                                     'InstallSize': [0],
                                     'ProductClass': ['11']
                                     })
        res, _ = obj_install_base.preprocess_expand_range(df_data_install, df_srnum)

        assert exp_res.equals(res)


class TestMergeBOMData:
    """
        Test proper merging of BOM data
    """

    @pytest.mark.parametrize(
        "df_bom",
        [None,
         [],
         {},
         (pd.DataFrame()),
         (pd.DataFrame(data={'Job_Index': [123, 121, 342]})),
         (pd.DataFrame(data={'PartNumber_TLN_BOM': [123, 121, 342]}))
         ])
    def test_merge_bom_data_error_1(self, df_bom):
        """
        Provided "df_bom" with
        None DataFrame
        Empty DataFrame
        Missing Columns "PartNumber_TLN_BOM"
        Missing Columns "Job_Index", should throw errors
        """
        df_data_install = pd.DataFrame(data={'Job_Index': [123, 121, 342],
                                             'f_all': [True, True, True],
                                             'Shipper_Index': [123, 342, 321],
                                             'ShipperItem_Index': [1, 2, 3]})
        merge_type = "inner"
        with pytest.raises(Exception) as _:
            obj_install_base.merge_bomdata(df_bom, df_data_install, merge_type)

    @pytest.mark.parametrize(
        "df_data_install",
        [None,
         [],
         {},
         (pd.DataFrame()),
         (pd.DataFrame(data={'f_all': [True, True, True],
                             'Shipper_Index': [123, 342, 321],
                             'ShipperItem_Index': [1, 2, 3], }))
         ])
    def test_merge_bom_data_error_2(self, df_data_install):
        """
        Provided "df_data_install" with
        None DataFrame
        Empty DataFrame
        Missing Columns "Job_Index", should throw errors
        """
        df_bom = pd.DataFrame(data={'Job_Index': [123, 121, 342],
                                    'PartNumber_TLN_BOM': [123, 121, 342]})
        merge_type = "inner"
        with pytest.raises(Exception) as _:
            obj_install_base.merge_bomdata(df_bom, df_data_install, merge_type)

    @pytest.mark.parametrize(
        "merge_type",
        [None,
         " ",
         [],
         {}
         ])
    def test_merge_bom_data_error_3(self, merge_type):
        """
        Provided "merge_type" with
        None value
        Empty string, should throw errors
        """
        df_data_install = pd.DataFrame(data={'Job_Index': [123, 121, 123],
                                             'f_all': [True, True, True],
                                             'Shipper_Index': [123, 342, 321],
                                             'ShipperItem_Index': [1, 2, 3]})
        df_bom = pd.DataFrame(data={'Job_Index': [123, 121, 123],
                                    'PartNumber_TLN_BOM': [1231, 2121, 1342]})
        with pytest.raises(Exception) as _:
            obj_install_base.merge_bomdata(df_bom, df_data_install, merge_type)

    def test_merge_bom_data_ideal_scenario(self):
        """
        Test proper data merge and fill na with empty string
        """
        df_data_install = pd.DataFrame(data={'Job_Index': [123, 121, 120],
                                             'ShipperItem_Index': [1, 2, 3]})
        df_bom = pd.DataFrame(data={'Job_Index': [123, 121, 124],
                                    'PartNumber_TLN_BOM': ['1231', None, 'ND1342']})
        merge_type = 'inner'
        res = obj_install_base.merge_bomdata(df_bom, df_data_install, merge_type)
        exp_res = pd.DataFrame(data={'Job_Index': [123, 121],
                                     'ShipperItem_Index': [1, 2],
                                     'PartNumber_TLN_BOM': ['1231', '']})

        assert exp_res.equals(res)


class TestMergeCustomerData:
    """
        Test proper merging of Customer data
    """

    @pytest.mark.parametrize(
        "df_data_install",
        [None,
         [],
         {},
         (pd.DataFrame()),
         (pd.DataFrame(data={'Customer': ['xyz', 'abc', 'mno']})),
         (pd.DataFrame(data={'ShipTo_Customer': ['abc', 'qwe', 'asd']}))
         ])
    def test_merge_customer_data_errors_1(self, df_data_install):
        """
        Provided "df_data_install" with
        None DataFrame
        Empty DataFrame
        Missing Columns "ShipTo_Customer"
        Missing Columns "Customer", should throw errors
        """
        df_customer = pd.DataFrame(data={'key': ['xyz:abc', 'abc:qwe', 'mno:asd'],
                                         'StrategicCustomer': ['abczsx',
                                                               'erddqwe', 'asktigd']})
        with pytest.raises(Exception) as _:
            obj_install_base.merge_customdata(df_customer, df_data_install)

    @pytest.mark.parametrize(
        "df_customer",
        [None,
         [],
         {},
         (pd.DataFrame()),
         (pd.DataFrame(data={'key': ['xyz:abc', 'abc:qwe', 'mno:asd']})),
         (pd.DataFrame(data={'StrategicCustomer': ['abczsx', 'erddqwe', 'asktigd']}))
         ])
    def test_merge_customer_data1_errors_2(self, df_customer):
        """
        Provided "df_customer" with
        None DataFrame
        Empty DataFrame
        Missing Columns "StrategicCustomer"
        Missing Columns "key", should throw errors
        """
        df_data_install = pd.DataFrame(
            data={'Customer': ['xyz', 'abc', 'mno'],
                  'ShipTo_Customer': ['abc', 'qwe', 'asd']})
        with pytest.raises(Exception) as _:
            obj_install_base.merge_customdata(df_customer, df_data_install)

    def test_merge_customer_data_ideal_scenario(self):
        """
        Test proper data merge with removal of duplicate values
        """
        df_customer = pd.DataFrame(
            data={'Serial_Number': ['1', '3'],
                  'StrategicCustomer': ['2', '4']})
        df_data_install = pd.DataFrame(
            data={'SerialNumber_M2M': ['1', '2', '3'],
                  'info': ['info1', 'info2', 'info3']})
        exp_res = pd.DataFrame(pd.DataFrame(
            data={'SerialNumber_M2M': ['1', '2', '3'],
                  'info': ['info1', 'info2', 'info3'],
                  'Serial_Number': ['1', None, '3'],
                  'StrategicCustomer': ['2', None, '4']}))
        res = obj_install_base.merge_customdata(df_customer, df_data_install)

        assert exp_res.equals(res)


class TestCleanSerialNumber:
    """
        Test proper cleaning of serial number data
    """

    @pytest.mark.parametrize(
        "df_srnum",
        [None,
         [],
         {},
         (pd.DataFrame()),
         'abc',
         ['avc', 21],
         54,
         (pd.DataFrame(data={'Shipper_Index': [123, 342, 321]})),
         ])
    def test_clean_serial_number_errors_1(self, df_srnum):
        """
        Provided "df_srnum" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns "Shipper_Index", should throw errors
        """
        with pytest.raises(Exception) as _:
            obj_install_base.clean_serialnum(df_srnum)

    def test_clean_serial_number_ideal_scenario(self):
        """
        Provided invalid "SerialNumber" which should be filtered
        """
        df_srnum = pd.DataFrame(data={
            'SerialNumber': ['@081-231-1 3', '0b1-23c 11-3!',
                             'p81 o31-1 6', '1b8-11g--1-5', '-110-1m0-1-10',
                             '-118-110-1-14-']})
        exp_res = pd.DataFrame(data={
            'SerialNumber': ['081-231-1-3', '0b1-23c-11-3',
                             'p81-o31-1-6', '1b8-11g-1-5', '110-1m0-1-10',
                             '118-110-1-14']})
        res = obj_install_base.clean_serialnum(df_srnum)

        assert exp_res.equals(res)

    def test_clean_serial_number_ideal_scenario_2(self):
        """
        Provided invalid "SerialNumber" which should be filtered
        """
        df_srnum = pd.DataFrame(data={
            'SerialNumber': ['@081-231-1-3', ' -0b1-23c-11-3',
                             'p81-o31-1-6-', '1b8-11g-1-5!', '110-1m0-1-10 ',
                             '&118-110-1-14', "  110-2894-7"]})
        exp_res = pd.DataFrame(data={
            'SerialNumber': ['081-231-1-3', '0b1-23c-11-3',
                             'p81-o31-1-6', '1b8-11g-1-5', '110-1m0-1-10',
                             '118-110-1-14', "110-2894-7"]})
        res = obj_install_base.clean_serialnum(df_srnum)
        print(res)
        assert exp_res.equals(res)


class TestCreateForeignKey:
    """
            Test foreign key generation for serial number data
    """

    @pytest.mark.parametrize(
        "df_srnum",
        [None,
         [],
         {},
         (pd.DataFrame()),
         'abc',
         ['avc', 21],
         54,
         (pd.DataFrame(data={'valid_sr': [True, True, True],
                             'ShipperItem_Index': [1, 2, 3]})),
         (pd.DataFrame(data={'Shipper_Index': [123, 342, 321],
                             'ShipperItem_Index': [1, 2, 3]})),
         (pd.DataFrame(data={'Shipper_Index': [1, 2, 3],
                             'valid_sr': [True, True, True], })),
         ])
    def test_create_foreign_key_errors_1(self, df_srnum):
        """
        Provided "df_srnum" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns "Shipper_Index"
        Missing Columns "valid_sr"
        Missing Columns "ShipperItem_Index", should throw errors
        """
        with pytest.raises(Exception) as _:
            obj_install_base.create_foreignkey(df_srnum)

    def test_create_foreign_key_ideal_scenario(self):
        """
        Provided False value to valid_sr to check filter and generation of key_serial
        """
        df_srnum = pd.DataFrame(data={'valid_sr': [True, True, False],
                                      'Shipper_Index': [123, 342, 321],
                                      'ShipperItem_Index': [1, 2, 3]})
        exp_res = pd.DataFrame(data={'valid_sr': [True, True],
                                     'Shipper_Index': [123, 342],
                                     'ShipperItem_Index': [1, 2],
                                     'key_serial': ['123:1', '342:2']})
        res = obj_install_base.create_foreignkey(df_srnum)

        assert exp_res.equals(res)

class TestIDDisplayParts:
    """
    Tests display panel details
    """
    @pytest.mark.parametrize(
        "df_org",
        [None,
         [],
         {},
         (pd.DataFrame())
         ]
    )
    def test_display_parts_err(self, df_org):
        """
        Provided "df_org" with
        None DataFrame
        Empty DataFrame
        Missing Columns in DataFrame all columns except one, should throw error
        """
        with pytest.raises(Exception) as _:
            obj_install_base.id_display_parts(df_org)


    def test_display_parts_ideal_val(self):
        """
        Provided "df_org" with ideal values for processing.
        """
        data = {'Job_Index': ['01322-0000','01422-0000','29612-0000','12033-0000','32892-0000','32895-02440'],
                'PartNumber_TLN_BOM': ['standard_pdu', 'rpp', 'wavestar primary system', 'wavestar primary pdu',
                                       'powerpak standard pdu', 'powerpak freestanding rpp'],
                'PartNumber_BOM_BOM': ['lug-54155', 'pnl08804', 'bkt12556', 'dor08880', 'bkt10430', 'dor09240'],
                'Total_Quantity': [2.00, 80.00, 6.00, 6.00, 6.00, 28.00]
            }

        expected_op = {
            'Job_Index': ['01322-0000', '01422-0000', '29612-0000', '12033-0000', '32892-0000', '32895-02440'],
            'pn_logic_tray': [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            'is_valid_logic_tray_lead': [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            'pn_door_assembly': [np.nan, np.nan, np.nan, '(# Other Parts: 1)', np.nan, 'DOR09240'],
            'is_valid_door_assembly_lead': [np.nan, np.nan, np.nan, 0.0, np.nan, 1.0],
            'pn_input_breaker_panel': [np.nan, 'PNL08804', np.nan, np.nan, np.nan, np.nan],
            'is_valid_input_breaker_panel_lead': [np.nan, 1.0, np.nan, np.nan, np.nan, np.nan],
            'pn_chasis': ['(# Other Parts: 1)']*6,
            'is_valid_chasis_lead': [0]*6
            }

        expected_df = pd.DataFrame(expected_op)
        df_org = pd.DataFrame(data)
        df_out = obj_install_base.id_display_parts(df_org)
        assert_frame_equal(df_out.sort_index(axis=1), expected_df.sort_index(axis=1),
                           check_dtype=False, check_exact=False, check_names=True)
        with pytest.raises(Exception) as info:
            obj_install_base.id_display_parts()
            assert info.type == Exception

class TestIDMetadata():
    """
    Tests ID metadata
    """
    @pytest.mark.parametrize(
        "df_bom",
        [None,
         [],
         {},
         (pd.DataFrame())
         ], )
    def test_id_metadata_err(self, df_bom):
        """

        None DataFrame
        Empty DataFrame
        Missing Columns in DataFrame all columns except one, should throw error
        """
        with pytest.raises(Exception) as _:
            result = obj_install_base.id_metadata(df_bom)

    def test_id_metadata_ideal_val(self):
        """

        :return:
        """
        bom_data = [{'Job_Index': '01322-0000', 'PartNumber_TLN_BOM': 'standard pdu'}, {'Job_Index': '01371-0000', 'PartNumber_TLN_BOM': 'standard pdu'}, {'Job_Index': '01375-0000', 'PartNumber_TLN_BOM': 'standard pdu'}, {'Job_Index': '01377-0000', 'PartNumber_TLN_BOM': 'standard pdu'}, {'Job_Index': '01433-0000', 'PartNumber_TLN_BOM': 'standard pdu'}]
        df_bom = pd.DataFrame(bom_data)
        result = obj_install_base.id_metadata(df_bom)
        expected_output = pd.DataFrame([{'Job_Index': '01322-0000', 'PartNumber_TLN_BOM': 'standard pdu', 'Ratings': np.nan},
         {'Job_Index': '01371-0000', 'PartNumber_TLN_BOM': 'standard pdu', 'Ratings': np.nan},
         {'Job_Index': '01375-0000', 'PartNumber_TLN_BOM': 'standard pdu', 'Ratings': np.nan},
         {'Job_Index': '01377-0000', 'PartNumber_TLN_BOM': 'standard pdu', 'Ratings': np.nan},
         {'Job_Index': '01433-0000', 'PartNumber_TLN_BOM': 'standard pdu', 'Ratings': np.nan}])

        assert_frame_equal(result.sort_index(axis=1), expected_output.sort_index(axis=1),
                           check_dtype=False, check_exact=False, check_names=True)

class TestIDMainBreaker():
    """
    Tests ID Main Breaker
    """
    @pytest.mark.parametrize(
        "df_install",
        [None,
         {},
         [],
         (pd.DataFrame())
         ], )
    def test_id_main_breaker_err(self, df_install):
        """

        None DataFrame
        Empty DataFrame
        Missing Columns in DataFrame all columns except one, should throw error
        """
        with pytest.raises(Exception) as _:
            df_data = obj_install_base.id_main_breaker(df_install)

    @pytest.mark.parametrize(
        "df_install, exp_op",
        [
            (
                pd.DataFrame(data={
                    "Job_Index": ["123"],
                    "PartNumber_TLN_BOM": ["123123"],
                    "PartNumber_BOM_BOM": ["ckb000036"],
                    "Total_Quantity": ["1"]
                }),
                pd.DataFrame(data={
                    "Job_Index": ["123"],
                    "pn_main_braker": ["ckb000036"],
                    "Input CB": ["Input CB"],
                    "AMP Trip": ["800A"],
                    "kAIC @ Voltage": ["65/480V"],
                    "Rated, Trip": ["100%, LSI"],
                    "kVA": [500.0],
                    "MFR": ["ETN"]
                })
            )
        ]
    )
    def test_id_main_breaker_ideal_val(self, df_install, exp_op):
        ac_op = obj_install_base.id_main_breaker(df_install)
        assert_frame_equal(exp_op.sort_index(axis=1),
                           ac_op.sort_index(axis=1),
                           check_dtype=False, check_exact=False,
                           check_names=True)

class TestSummarizePartNumbers():
    @pytest.mark.parametrize(
        "part_list, list_of_interest",
        [
            (None, None),
            ({}, {}),
            (pd.DataFrame(), pd.DataFrame())
        ]
    )
    def test_summarize_part_numbers_err(self, part_list, list_of_interest):
        with pytest.raises(Exception) as _:
            out = obj_install_base.summarize_part_num(
                part_list, list_of_interest
            )

    @pytest.mark.parametrize(
        "part_list, list_of_interest, exp_op",
        [
            (
                ["test1", "test2"],
                ["test2"],
                'TEST2 (# Other Parts: 1)'
            ),
            (
                ["test1", "test2", "test3", "test4"],
                ["test2", "test3"],
                'TEST2, TEST3 (# Other Parts: 2)'
            )
        ]
    )
    def test_err_summarize_part_no(self, part_list, list_of_interest, exp_op):
        ac_op = obj_install_base.summarize_part_num(
            part_list, list_of_interest
        )
        assert ac_op == exp_op

class TestGetMetadata():
    @pytest.mark.parametrize(
        "df_data_install",
        [
            None,
            {},
            list(),
            pd.DataFrame()
        ]
    )
    def test_err_summarize_part_numbers(self, df_data_install):
        with pytest.raises(Exception) as _:
            df_data_install = obj_install_base.get_metadata(df_data_install)

    def test_config_not_load(self):
        """
        This test cases checks all the inputs for which the method can't find
        dependent files.
        @param dict_src: input source type
        @param df_data: input dataframe
        @return: None
        """
        df_data_install = pd.DataFrame({
            'Description': ["Test input 100amp, 220 v, 110 kva"]
        })
        kva_search_pattern = (
            obj_install_base.config['install_base']['kva_search_pattern']
        )
        del obj_install_base.config['install_base']['kva_search_pattern']
        with pytest.raises(Exception) as _:
            _ = obj_install_base.get_metadata(df_data_install)
        obj_install_base.config['install_base']['kva_search_pattern'] = (
            kva_search_pattern
        )

    @pytest.mark.parametrize(
        "df_data_install, exp_op",
        [
            (
                pd.DataFrame({
                    'Description': ["Test input 100amp, 220 v, 110 kva"]
                }),
                pd.DataFrame({
                    'Description': ["Test input 100amp, 220 v, 110 kva"],
                    'kva': [['110 kva']],
                    'amp': [['100amp']],
                    'voltage': [['220 v']]
                })
            )
        ]
    )
    def test_err_summarize_part_no(self, df_data_install, exp_op):
        ac_op = obj_install_base.get_metadata(df_data_install)
        assert_frame_equal(exp_op.sort_index(axis=1),
                           ac_op.sort_index(axis=1),
                           check_dtype=False, check_exact=False,
                           check_names=True)

class TestIdentifyProductForSerial():
    @pytest.mark.parametrize(
        "ar_tln",
        [
            None,
            {},
            list(),
            pd.DataFrame()
        ]
    )
    def test_err_identify_prdt_for_serial(self, ar_tln):
        with pytest.raises(Exception) as _:
            _ = obj_bus_logic.idetify_product_fr_serial(
                ar_tln)

    @pytest.mark.parametrize(
        "ar_tln, ex_op",
        [
            (pd.Series(["150-123-3-4"]), pd.Series(["PDU"])),
            (pd.Series(["350-123-3-4"]), pd.Series(["PDU - Primary"])),
            (pd.Series(["420-123-3-4"]), pd.Series(["PDU - Secondary"])),
            (pd.Series(["54-123-3-4"]), pd.Series(["Reactor"])),
            (pd.Series(["180-123-3-4"]), pd.Series(["RPP"])),
            (pd.Series(["410-123-3-4"]), pd.Series(["STS"]))
        ]
    )
    def test_err_identify_prdt_for_Serial(self, ar_tln, ex_op):
        ac_op = obj_bus_logic.idetify_product_fr_serial(
            ar_tln)
        assert ac_op[0] == ex_op[0]

#%%
if __name__ == "__main__":
    testclass = TestPrioratizedColumn()
