"""@file test_class_contract_data.py.

@brief This file used to test code for Contract Data




@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

import pytest
import pandas as pd
from pandas import Timestamp, NaT
from pandas._testing import assert_frame_equal

# from src.class_help_filters import ConfigureFilts
# from src.class_help_logger import AppLogger
# from src.class_help_setup import SetupEnvironment
# from src.class_contracts_data import Contract
from utils.dcpd.class_contracts_data import Contract

# import src.config_set as conf_

# env_ = SetupEnvironment('DCPD', conf_.dict_)
# CONF = env_.indentify_env()
# logger = AppLogger('DCPD', level='')
# filters_ = ConfigureFilts(logger)

obj_contract = Contract()


class TestFilterSrnum:
    """
    Check invalid serial numbers getting filtered.
    """

    @pytest.mark.parametrize(
        "df_srnum",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={'SerialNumber': [3, 1, None, 4]})),
         (pd.DataFrame(data={'ContractNumber': [21, None, 43, None]})),
         (pd.DataFrame(data={'SerialNumberContract': [None, 10, None, 98]})),
         (pd.DataFrame(data={'Qty': [30, 2, '', 5]})),
         (pd.DataFrame(data={'src': [30, 2, '', 5]}))
         ])
    def test_filter_srnum_errors_1(self, df_srnum):
        """
        Provided "df_srnum" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        with pytest.raises(Exception) as _:
            obj_contract.filter_srnum(df_srnum)

    def test_filter_srnum_scenario_1(self):
        """
        Check invalid serial numbers getting filtered.
        """
        df_data = pd.DataFrame(data={
            'SerialNumber': ['110-0143', '110-0057', '411-0247', 'cl110-4363',
                             '411-0247', '110-4363', '110-1959-1-64',
                             '120-0146-1-2', '120-0254-1-2', '120-0024A-E',
                             None, '180-0514-1-32', '110-1520-1-12',
                             '450-0017-1-4', '110-2046-1-6', '180-0897-1-6',
                             None, None, None, None, None, None, None,
                             None,
                             None, None, None, None, None, None, None, None,
                             None, None, None, None, None],
            'ContractNumber': [5656, 5656, 5614, 5614, 4981, 4981, 4946, 5566,
                               5566, 5566, 4966, 5656, 5656, 5656, 5656,
                               5656, 5656, 5614, 4981, 4946, 5566, 4966, 5656,
                               5656, 5614, 4981, 4946, 5566, 4966, 5656,
                               5656, 5614, 4981, 4946, 5566, 4966, 5656],
            'SerialNumberContract': ['110-0143 / 110-0057', '110-0143 / 110-0057',
                                     '411-0247 & cl110-4363',
                                     '411-0247 & cl110-4363', '411-0247                   110-4363',
                                     '411-0247                   110-4363', '110-1959-1-64',
                                     '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                     '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                     '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                     "(20) Secondary Switch Systems.  (145) RPP's.  See serial #'s in contract folder.",
                                     '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                     '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                     '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                     '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                     '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                     '', '', '',
                                     '',
                                     '', '', '', '', '', '', '', '', '', '',
                                     '', '', '', '', '', '', ''],
            'Qty': [17, 17, 9, 9, 9, 9, 165, 60, 60, 60, 4, 1, 1, 1, 1, 1, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0,
                    0, 0, 0, 0, 0],
            'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                    'Product_1_Serial__c', 'Product_1_Serial__c',
                    'Product_1_Serial__c', 'Product_1_Serial__c',
                    'Product_1_Serial__c', 'Product_1_Serial__c',
                    'Product_1_Serial__c', 'Product_1_Serial__c',
                    'Product_1_Serial__c', 'Product_1_Serial__c',
                    'Product_1_Serial__c', 'Product_1_Serial__c',
                    'Product_1_Serial__c', 'Product_1_Serial__c',
                    'Product_2_Serial__c', 'Product_2_Serial__c',
                    'Product_2_Serial__c', 'Product_2_Serial__c',
                    'Product_2_Serial__c', 'Product_2_Serial__c',
                    'Product_2_Serial__c', 'Product_3_Serial__c',
                    'Product_3_Serial__c', 'Product_3_Serial__c',
                    'Product_3_Serial__c', 'Product_3_Serial__c',
                    'Product_3_Serial__c', 'Product_3_Serial__c',
                    'Contract_Comments__c', 'Contract_Comments__c',
                    'Contract_Comments__c', 'Contract_Comments__c',
                    'Contract_Comments__c', 'Contract_Comments__c',
                    'Contract_Comments__c'
                    ]})
        res = obj_contract.filter_srnum(df_data)
        exp_res = pd.DataFrame(
            data={'SerialNumber': ['110-0143', '110-0057', '411-0247', '411-0247',
                                   '110-4363', '110-1959-1-64',
                                   '120-0146-1-2', '120-0254-1-2', '120-0024A-E',
                                   '180-0514-1-32', '110-1520-1-12',
                                   '450-0017-1-4', '110-2046-1-6', '180-0897-1-6'],
                  'ContractNumber': [5656, 5656, 5614, 4981, 4981, 4946, 5566,
                                     5566, 5566, 5656, 5656, 5656, 5656,
                                     5656],
                  'SerialNumberContract': ['110-0143 / 110-0057',
                                           '110-0143 / 110-0057', '411-0247 & cl110-4363',
                                           '411-0247                   110-4363',
                                           '411-0247                   110-4363', '110-1959-1-64',
                                           '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                           '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                           '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6'],
                  'Qty': [17, 17, 9, 9, 9, 165, 60, 60, 60, 1, 1, 1, 1, 1],
                  'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c'],
                  'Product': ['PDU', 'PDU', 'STS', 'STS', 'PDU', 'PDU',
                              'PDU', 'PDU', 'PDU', 'RPP', 'PDU',
                              'PDU - Secondary', 'PDU', 'RPP']})
        assert exp_res.equals(res)


class TestFlagSrnumRange:
    """
    Check True flag is assign when serial number has range otherwise False
    """

    @pytest.mark.parametrize(
        "df_data",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={'SerialNumber': ['110-0143', '110-0057',
                                              '110-1959-1-64', '120-0024A-E']})),
         (pd.DataFrame(data={'ContractNumber': [5656, 5656, 5614, 4981]})),
         (pd.DataFrame(data={'SerialNumberContract': ['110-0143 / 110-0057',
                                                      '110-0143 / 110-0057', '110-1959-1-64',
                                                      '120-0024A-E & 120-0146-1-2; 120-0254-1-2']})),
         (pd.DataFrame(data={'Qty': [0, 0, 64, 5]})),
         (pd.DataFrame(data={'Product': ['PDU', 'PDU', 'STS', 'STS']})),
         (pd.DataFrame(data={
             'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                     'Product_1_Serial__c', 'Product_1_Serial__c']}))
         ])
    def test_flag_serialnumber_wid_range_errors_1(self, df_data):
        """
        Provided "df" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        with pytest.raises(Exception) as _:
            obj_contract.flag_serialnumber_wid_range(df_data)

    def test_flag_serialnumber_wid_range_ideal_scenario_1(self):
        """
        Check True flag is assign when serial number has range otherwise False
        """
        df_data = pd.DataFrame(
            data={'SerialNumber': ['110-3763 (unit 2 & 3', '180-1931 (1-4,13-15,24',
                                   '110-2981 (12, 18', '110-3780-5',
                                   '110-2837 (12 & 13', '110-3745-14', '420-0023'],
                  'ContractNumber': ['5987', '6001', '6237', '5722', '5184',
                                     '5773', '6801'],
                  'SerialNumberContract': ['110-3763 (unit 2 & 3)',
                                           '180-1931 (1-4,13-15,24)', '110-2981 (12, 18)',
                                           '110-3780-5,9-12, 13, 15 & 16',
                                           '110-2837 (12 & 13)',
                                           '110-3745-14,8,9,15,12,11, 6, ??',
                                           '(16) STS; 420-0023-1-16 (110-0659-1-32)'],
                  'Qty': [2, 8, 2, 8, 2, 8, 1],
                  'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c'],
                  'Product': ['PDU', 'RPP', 'PDU', 'PDU', 'PDU', 'PDU',
                              'PDU - Secondary']})
        res = obj_contract.flag_serialnumber_wid_range(df_data)
        exp_res = pd.Series([False, False, False, True, False, True, True])

        assert exp_res.equals(res)

    def test_flag_serialnumber_wid_range_range_ideal_scenario_2(self):
        """
        Check True flag is assign when serial number has range otherwise False
        """
        df_data = pd.DataFrame(
            data={'SerialNumber': ['110-0143', '110-0057', '411-0247',
                                   '411-0247', '110-4363', '110-1959-1-64',
                                   '120-0146-1-2', '120-0254-1-2',
                                   '120-0024A-E', '180-0514-1-32', '110-1520-1-12',
                                   '450-0017-1-4', '110-2046-1-6', '180-0897-1-6'],
                  'ContractNumber': [5656, 5656, 5614, 4981, 4981, 4946, 5566,
                                     5566, 5566, 5656, 5656, 5656, 5656,
                                     5656],
                  'SerialNumberContract': ['110-0143 / 110-0057',
                                           '110-0143 / 110-0057', '411-0247 & cl110-4363',
                                           '411-0247                   110-4363',
                                           '411-0247                   110-4363',
                                           '110-1959-1-64',
                                           '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                           '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                           '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6',
                                           '180-0514-1-32 110-1520-1-12 450-0017-1-4 110-2046-1-6 180-0897-1-6'],
                  'Qty': [0, 0, 0, 0, 0, 64, 2, 2, 1, 32, 12, 4, 6, 6],
                  'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c',
                          'Product_1_Serial__c', 'Product_1_Serial__c'],
                  'Product': ['PDU', 'PDU', 'STS', 'STS', 'PDU', 'PDU',
                              'PDU', 'PDU', 'PDU', 'RPP', 'PDU',
                              'PDU - Secondary', 'PDU', 'RPP']})
        res = obj_contract.flag_serialnumber_wid_range(df_data)
        exp_res = pd.Series(
            [True, True, True, True, True, False, False, False, True,
             False, False, False, False, False])
        assert exp_res.equals(res)


class TestSepSingleMulSrnum:
    """
    Check serialnumber is getting spilt correctly into single and multiple range serialnumber.
    """

    @pytest.mark.parametrize(
        "df_srnum",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={'SerialNumber': ['110-0143', '110-0057',
                                              '110-1959-1-64', '120-0024A-E']})),
         (pd.DataFrame(data={'ContractNumber': [5656, 5656, 5614, 4981]})),
         (pd.DataFrame(data={'SerialNumberContract': ['110-0143 / 110-0057',
                                                      '110-0143 / 110-0057', '110-1959-1-64',
                                                      '120-0024A-E & 120-0146-1-2; 120-0254-1-2']})),
         (pd.DataFrame(data={'Qty': [0, 0, 64, 5]})),
         (pd.DataFrame(data={'Product': ['PDU', 'PDU', 'STS', 'STS']})),
         (pd.DataFrame(data={
             'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                     'Product_1_Serial__c', 'Product_1_Serial__c']})),
         (pd.DataFrame(data={'is_single': [False, False, True, True]}))
         ])
    def test_sep_single_mul_srnum_errors_1(self, df_srnum):
        """
        Provided "df_srnum" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        ls_cols = ['ContractNumber', 'SerialNumberContract', 'Qty',
                   'Product', 'SerialNumber', 'src']
        with pytest.raises(Exception) as _:
            obj_contract.sep_single_mul_srnum(df_srnum, ls_cols)

    def test_sep_single_mul_srnum_ideal_scenario(self):
        """
        Check serialnumber is getting spilt correctly into single and multiple range serialnumber.
        """
        ls_cols = ['ContractNumber', 'SerialNumberContract', 'Qty',
                   'Product', 'SerialNumber', 'src']
        df_data = pd.DataFrame(data={'SerialNumber': ['110-0143-1', '120-0024A-E'],
                                     'ContractNumber': [5656, 4981],
                                     'SerialNumberContract': ['110-0143-1',
                                                              '120-0024A-E & 120-0146-1-2; 120-0254-1-2'],
                                     'Qty': [1, 5],
                                     'Product': ['PDU', 'STS'],
                                     'src': ['Product_1_Serial__c',
                                             'Product_1_Serial__c'],
                                     'is_single': [True, False]})

        single, multiple = obj_contract.sep_single_mul_srnum(df_data, ls_cols)
        multiple.reset_index(drop=True, inplace=True)
        exp_single = pd.DataFrame(data={'ContractNumber': [5656],
                                        'SerialNumberContract': ['110-0143-1'],
                                        'Qty': [1],
                                        'Product': ['PDU'],
                                        'SerialNumber': ['110-0143-1'],
                                        'src': ['Product_1_Serial__c']})

        exp_multiple = pd.DataFrame(data={'ContractNumber': [4981],
                                          'SerialNumberContract': [
                                              '120-0024A-E & 120-0146-1-2; 120-0254-1-2'],
                                          'Qty': [5],
                                          'Product': ['STS'],
                                          'SerialNumber': ['120-0024A-E'],
                                          'src': ['Product_1_Serial__c']})
        exp_multiple.reset_index(drop=True, inplace=True)

        assert exp_single.equals(single)
        assert exp_multiple.equals(multiple)


class TestGetRangeSrum:
    """
    Check serialnumber is correctly getting expand.
    """

    @pytest.mark.parametrize(
        "df_srnum",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={'SerialNumber': ['110-0143', '110-0057',
                                              '110-1959-1-64', '120-0024A-E']})),
         (pd.DataFrame(data={'ContractNumber': [5656, 5656, 5614, 4981]})),
         (pd.DataFrame(data={'SerialNumberContract': ['110-0143 / 110-0057',
                                                      '110-0143 / 110-0057', '110-1959-1-64',
                                                      '120-0024A-E & 120-0146-1-2; 120-0254-1-2']})),
         (pd.DataFrame(data={'Qty': [0, 0, 64, 5]})),
         (pd.DataFrame(data={'Product': ['PDU', 'PDU', 'STS', 'STS']})),
         (pd.DataFrame(data={
             'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                     'Product_1_Serial__c', 'Product_1_Serial__c']}))
         ])
    def test_get_range_srum_errors_1(self, df_srnum):
        """
        Provided "df_srnum" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        with pytest.raises(Exception) as info:
            obj_contract.get_range_srum(df_srnum)
            assert info.type == Exception

    def test_get_range_srum_ideal_scenario(self):
        """
        Check serialnumber is correctly getting expand.
        """
        ls_cols = ['ContractNumber', 'SerialNumberContract', 'Qty',
                   'Product', 'SerialNumber', 'src']
        df_data = pd.DataFrame(data={'ContractNumber': [4981],
                                     'SerialNumberContract': [
                                         '120-0024A-E & 120-0146-1-2; 120-0254-1-2'],
                                     'Qty': [5],
                                     'Product': ['STS'],
                                     'SerialNumber': ['120-0024A-E'],
                                     'src': ['Product_1_Serial__c']})

        res = obj_contract.get_range_srum(df_data[ls_cols])
        exp_res = pd.DataFrame(data={
            'ContractNumber': [4981, 4981, 4981, 4981, 4981],
            'SerialNumberContract': ['120-0024A-E & 120-0146-1-2; 120-0254-1-2', '120-0024A-E & 120-0146-1-2; '
                                    '120-0254-1-2', '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                     '120-0024A-E & 120-0146-1-2; 120-0254-1-2', '120-0024A-E & 120-0146-1-2; '
                                                                                 '120-0254-1-2']
            ,'Qty': [5, 5, 5, 5, 5], 'Product': ['STS', 'STS', 'STS', 'STS', 'STS']
            , 'SerialNumberOrg': ['120-0024a-e', '120-0024a-e', '120-0024a-e', '120-0024a-e', '120-0024a-e']
            , 'src': ['Product_1_Serial__c', 'Product_1_Serial__c', 'Product_1_Serial__c', 'Product_1_Serial__c',
                      'Product_1_Serial__c'], 'SerialNumber': ['120-0024-a', '120-0024-b', '120-0024-c', '120-0024-d',
                                                               '120-0024-e'], 'KeySerial': ['contract', 'contract',
                                                                                            'contract', 'contract',
                                                                                            'contract']})

        assert exp_res.equals(res)


class TestConcatExportData:
    """
    Check if separated single and multiple range serial number data
    getting correctly concatenated.
    """

    @pytest.mark.parametrize(
        "df_out_sub_single",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={'SerialNumber': ['110-0143', '110-0057',
                                              '110-1959-1-64', '120-0024A-E']})),
         (pd.DataFrame(data={'SerialNumberOrg': ['120-0024a-e', '120-0024a-e',
                                                 '120-0024a-e',
                                                 '120-0024a-e',
                                                 '120-0024a-e']}))
         ])
    def test_concat_export_data_errors_1(self, df_out_sub_single):
        """
        Provided "df_out_sub_single" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        df_out_sub_multi = pd.DataFrame(data={'ContractNumber': [4981, 4981, 4981, 4981, 4981],
                                              'SerialNumberContract': [
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2'],
                                              'Qty': [5, 5, 5, 5, 5],
                                              'Product': ['STS', 'STS', 'STS', 'STS', 'STS'],
                                              'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                                                      'Product_1_Serial__c',
                                                      'Product_1_Serial__c', 'Product_1_Serial__c'],
                                              })
        with pytest.raises(Exception) as _:
            obj_contract.concat_export_data(df_out_sub_single, df_out_sub_multi)

    @pytest.mark.parametrize(
        "df_out_sub_multi",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={'SerialNumber': ['110-0143', '110-0057',
                                              '110-1959-1-64', '120-0024A-E']})),
         (pd.DataFrame(data={'SerialNumberOrg': ['120-0024a-e', '120-0024a-e',
                                                 '120-0024a-e',
                                                 '120-0024a-e',
                                                 '120-0024a-e']})),
         ])
    def test_concat_export_data_errors_2(self, df_out_sub_multi):
        """
        Provided "df_out_sub_single" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        df_out_sub_single = pd.DataFrame(data={'ContractNumber': [5656],
                                               'SerialNumberContract': ['110-0143-1'],
                                               'Qty': [1],
                                               'Product': ['PDU'],
                                               'src': ['Product_1_Serial__c']})
        with pytest.raises(Exception) as _:
            obj_contract.concat_export_data(df_out_sub_single, df_out_sub_multi)

    def test_concat_export_data_ideal_scenario(self):
        """
        Check if separated single and multiple range serial number data
        getting correctly concatenated.
        """
        df_out_sub_multi = pd.DataFrame(data={'ContractNumber': [4981, 4981,
                                                                 4981, 4981, 4981],
                                              'SerialNumberContract': [
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                  '120-0024A-E & 120-0146-1-2; 120-0254-1-2'],
                                              'Qty': [5, 5, 5, 5, 5],
                                              'Product': ['STS', 'STS', 'STS', 'STS', 'STS'],
                                              'SerialNumberOrg': ['120-0024a-e', '120-0024a-e',
                                                                  '120-0024a-e',
                                                                  '120-0024a-e',
                                                                  '120-0024a-e'],
                                              'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                                                      'Product_1_Serial__c',
                                                      'Product_1_Serial__c', 'Product_1_Serial__c'],
                                              'SerialNumber': ['120-0024a', '120-0024b',
                                                               '120-0024c', '120-0024d',
                                                               '120-0024e']})

        df_out_sub_single = pd.DataFrame(data={'ContractNumber': [5656],
                                               'SerialNumberContract': ['110-0143-1'],
                                               'Qty': [1],
                                               'Product': ['PDU'],
                                               'SerialNumber': ['110-0143-1'],
                                               'src': ['Product_1_Serial__c']})

        res = obj_contract.concat_export_data(df_out_sub_single, df_out_sub_multi)
        res.reset_index(drop=True, inplace=True)

        exp_res = pd.DataFrame(data={'ContractNumber': [5656, 4981, 4981, 4981,
                                                        4981, 4981],
                                     'SerialNumberContract': ['110-0143-1',
                                                              '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                              '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                              '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                              '120-0024A-E & 120-0146-1-2; 120-0254-1-2',
                                                              '120-0024A-E & 120-0146-1-2; 120-0254-1-2'],
                                     'Qty': [1, 5, 5, 5, 5, 5],
                                     'Product': ['PDU', 'STS', 'STS', 'STS', 'STS', 'STS'],
                                     'SerialNumber': ['110-0143-1', '120-0024a', '120-0024b',
                                                      '120-0024c',
                                                      '120-0024d',
                                                      '120-0024e'],
                                     'src': ['Product_1_Serial__c', 'Product_1_Serial__c',
                                             'Product_1_Serial__c',
                                             'Product_1_Serial__c',
                                             'Product_1_Serial__c', 'Product_1_Serial__c'],
                                     'SerialNumberOrg': [None, '120-0024a-e', '120-0024a-e',
                                                         '120-0024a-e',
                                                         '120-0024a-e', '120-0024a-e'],
                                     })
        exp_res.reset_index(drop=True, inplace=True)

        assert exp_res.equals(res)


class TestIdStartup:
    """
    Check if startup date is null then correctly filled with priority date columns.
    """

    @pytest.mark.parametrize(
        "df_srnum",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         ])
    def test_id_startup_errors_1(self, df_srnum):
        """
        Provided "df_srnum" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        with pytest.raises(Exception) as _:
            obj_contract.id_startup(df_srnum)

    def test_id_startup_ideal_scenario_1(self):
        """
        Check if startup date is null then correctly filled with priority date columns.
        """
        ls_cols_startup = [
            "Start_Up_Completed_Date__c",
            "Scheduled_Start_Up_Date__c"
        ]
        df_data = pd.DataFrame(data={'Start_Up_Completed_Date__c': [None,
                                                                    'Sunday, March 13, 2022', None,
                                                                    None],
                                     'Customer_Tentative_Start_Up_Date__c': [None,
                                                                             'Friday, May 31, 2012',
                                                                             None,
                                                                             'Friday, May 31, 2012'],
                                     'Scheduled_Start_Up_Date__c': [None, None,
                                                                    'Thursday, June 16, 2022',
                                                                    'Sunday, March 13, 2022']})
        df_data['Start_Up_Completed_Date__c'] = pd.to_datetime(
            df_data['Start_Up_Completed_Date__c'])
        df_data['Scheduled_Start_Up_Date__c'] = pd.to_datetime(
            df_data['Scheduled_Start_Up_Date__c'])
        df_data['Customer_Tentative_Start_Up_Date__c'] = pd.to_datetime(
            df_data['Customer_Tentative_Start_Up_Date__c'])
        res = obj_contract.id_startup(df_data[ls_cols_startup])
        exp_res = pd.DataFrame(data={'was_startedup': [False, True, True, True],
                                     'startup_date': [None, 'Sunday, March 13, 2022',
                                                      'Thursday, June 16, 2022',
                                                      'Sunday, March 13, 2022']})
        exp_res['startup_date'] = pd.to_datetime(exp_res['startup_date'])

        assert exp_res.equals(res)


class TestDecodeContractData:
    """
    Check if Contract service plan is correctly getting decoded.
    """

    @pytest.mark.parametrize(
        "df_contract_decode",
        [None,
         (pd.DataFrame()),
         'abc',
         [1, 2, 'abc'],
         1234
         ])
    def test_decode_contract_data_errors_1(self, df_contract_decode):
        """
        Provided "df_contract_decode" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        with pytest.raises(Exception) as _:
            obj_contract.decode_contract_data(df_contract_decode)

    def test_decode_contract_data_ideal_scenario_1(self):
        """
        Check if Contract service plan is correctly getting decoded.
        """
        df_data = pd.DataFrame(data={'Service_Plan': ['bronze-1', 'gold', 'silver',
                                                      'extended warranty', None]})
        res = obj_contract.decode_contract_data(df_data)

        exp_res = pd.DataFrame(data={
            'Eaton_ContractType': ['Flex TM Response Only Contract + Annual PM',
                                   'PowerTrust Preferred Plan',
                                   'PowerTrust Plan', 'Warranty Upgrade', '']})

        assert exp_res['Eaton_ContractType'].equals(res['Eaton_ContractType'])


class TestDecodeInstallbaseData:
    """
    Check if Contract service plan is correctly getting decoded.
    """

    @pytest.mark.parametrize(
        "df_install_decode",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={'SO': []})),
         (pd.DataFrame(data={'Description': []})),
         (pd.DataFrame(data={'Original_Sales_Order__c': []}))
         ])
    def test_decode_installbase_data_errors_1(self, df_install_decode):
        """
        Provided "df_install_decode" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        df_contract_decode = pd.DataFrame(
            data={'Service_Plan': ['Bronze-1', 'Gold', 'Silver',
                                   'Extended Warranty', None],
                  'Eaton_ContractType': ['Flex TM Response Only Contract + Annual PM',
                                         'PowerTrust Preferred Plan',
                                         'PowerTrust Plan',
                                         'Warranty Upgrade', '']})
        with pytest.raises(Exception) as _:
            obj_contract.decode_installbase_data(df_install_decode, df_contract_decode)

    def test_decode_installbase_data_ideal_scenario_1(self):
        """
        Check if Contract service plan is correctly getting decoded.
        """

        df_install_decode = pd.DataFrame(data={'SO': [1234, 7643, 6753, 9554, 5795],
                                               'Description': ["PM only service",
                                                               "Labor and Travel for 1/2 day (4 Hours on site + 2 Hours travel) during" \
                                                               " Prime Time (Weekend & Holidays)",
                                                               "Labor and Travel",
                                                               "1 preventive maintenance",
                                                               "Scope of Work:  Visit the site in San Diego to inspection one static" \
                                                               " switch system for proper operation."],
                                               })
        df_contract_decode = pd.DataFrame(
            data={'Service_Plan': ['bronze-1', 'gold', 'silver',
                            'extended warranty', None],
                  'Eaton_ContractType': ['Flex TM Response Only Contract + Annual PM',
                                         'PowerTrust Preferred Plan',
                                         'PowerTrust Plan',
                                         'Warranty Upgrade', ''],
                  'Original_Sales_Order__c': [1234, 7643, 6753, 9554, 5795]})
        res = obj_contract.decode_installbase_data(df_install_decode, df_contract_decode)
        exp_res = pd.DataFrame(data={'flag_update': [False, False, False,
                                                     False, True]})

        assert exp_res['flag_update'].equals(res['flag_update'])


# class TestValidateSrnum:
#     """
#     Check if contract serialnumber is correctly validated through installbase serialnumber
#     """
#
#     @pytest.mark.parametrize(
#         "df_contract_srnum",
#         [None,
#          (pd.DataFrame()),
#          'dcacac',
#          [123, 'aeda'],
#          1432,
#          ])
#     def test_validate_srnum_errors_1(self, df_contract_srnum):
#         """
#         Provided "df_install_decode" with
#         None DataFrame
#         Empty DataFrame
#         string value
#         list
#         numeric value
#         Missing Columns, should throw errors
#         """
#         df_install = pd.DataFrame(
#             data={'SerialNumber_M2M': ['185-0043-co', '110-2768',
#                                        '110-4525-1-8-expfee', '730-1868-b',
#                                        't18-26-us-s-4313',
#                                        '30958wa-5020-1'
#                                        ]})
#         with pytest.raises(Exception) as _:
#             obj_contract.validate_srnum(df_install, df_contract_srnum)
#
#     @pytest.mark.parametrize(
#         "df_install",
#         [None,
#          (pd.DataFrame()),
#          'dcacac',
#          [123, 'aeda'],
#          1432,
#          ])
#     def test_validate_srnum_errors_2(self, df_install):
#         """
#         Provided "df_install" with
#         None DataFrame
#         Empty DataFrame
#         string value
#         list
#         numeric value
#         Missing Columns, should throw errors
#         """
#         df_contract_srnum = pd.DataFrame(data={'SerialNumber': ['185', '110-2768',
#                                                                 '110-4525-1', '730-1868',
#                                                                 't18',
#                                                                 '30958wa', '000']})
#         with pytest.raises(Exception) as _:
#             obj_contract.validate_srnum(df_install, df_contract_srnum)
#
#     def test_validate_srnum_ideal_scenario(self):
#         """
#         Check if contract serialnumber is correctly validated through installbase serialnumber
#         """
#         df_install = pd.DataFrame(
#             data={'SerialNumber_M2M': ['185-0043-co', '110-2768', '110-4525-1-8-expfee',
#                                        '730-1868-b',
#                                        't18-26-us-s-4313',
#                                        '30958wa-5020-1'
#                                        ]})
#         df_contract_srnum = pd.DataFrame(data={'SerialNumber': ['185', '110-2768',
#                                                                 '110-4525-1', '730-1868',
#                                                                 't18',
#                                                                 '30958wa', '000']})
#         res = obj_contract.validate_srnum(df_install, df_contract_srnum)
#         exp_res = pd.DataFrame(data={'flag_validinstall': [True, True, True,
#                                                            True, True, True, False]})
#         assert exp_res['flag_validinstall'].equals(res['flag_validinstall'])


class TestMergeContractAndSrnum:
    """
    Check if contract data and serialnumber data is correctly getting merged.
    """

    @pytest.mark.parametrize(
        "df_contract_srnum",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={'Product': ['PDU', 'STS', 'RPP', 'PDU', 'PDU']})),
         ])
    def test_merge_contract_and_srnum_error(self, df_contract_srnum):
        """
        Provided "df_contract_srnum" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        df_contract = pd.DataFrame(data={
            "ContractNumber": [6827, 7783, 6539, 5767, 2341],
            "PDI_ContractType": ['new', 'new', 'existing', None, 'new'],
            "Service_Plan": ['gold', 'extended warranty', 'silver', 'gold', 'gold'],
            "Contract_Stage__c": ['warranty', 'closed', 'converted',
                                  'closed', 'warranty'],
            "StatusCode": ['activated', 'activated', 'draft', 'draft', 'activated'],
            "Original_Sales_Order__c": [2343, 4523, 6765, 6885, 3467],
            "PDI_Product_Family__c": ['pdu', 'static transfer switch',
                                      'rpp', 'pdu', 'pdu'],
            "was_startedup": [False, True, True, True, True],
            "startup_date": [None, '12/29/2020', '2/9/2000', '10/2/2010',
                             '8/10/2022'],
            "Start_Up_Completed_Date__c": [None, '12/29/2020', '2/9/2000',
                                           '10/2/2010', '8/10/2022'],
            "Customer_Tentative_Start_Up_Date__c": [None, None, None, None,
                                                    None],
            "Scheduled_Start_Up_Date__c": [None, None, None, None, None],
            "Warranty_Start_Date": [None, '12/29/2020', '2/9/2000', '10/2/2010',
                                    '8/10/2022'],
            "Warranty_Expiration_Date": [None, None, None, None, None],
            "Payment_Frequency": ['full amount', 'annual', 'full amount',
                                  'annual', 'annual'],
            "Start_date": [None, None, None, None, None], "BillingStreet": [None,
                                                                            None, None, None, None],
            "BillingCity": [None, None, None, None, None], "BillingState": [None,
                                                                            None, None, None, None],
            "BillingPostalCode": [None, None, None, None, None],
            "BillingCountry": [None, None, None, None, None],
            "BillingAddress": [None, None, None, None, None],
            "Country__c": [None, None, None, None, None],
            "Contract": [None, None, None, None, None],
            "Service_Sales_Manager": [None, None, None, None, None]})

        with pytest.raises(Exception) as info:
            obj_contract.merge_contract_and_srnum(df_contract, df_contract_srnum)
            assert info.type == Exception

    def test_merge_contract_and_srnum_ideal_scenario(self):
        """
        Check if contract data and serialnumber data is correctly getting merged.
        """
        df_contract_srnum = pd.DataFrame(data={
            'ContractNumber': [6827, 7783, 6539],
            'Product': ['PDU', 'STS', 'RPP'],
            'SerialNumber_Partial': ['110-432-1', '118-221-7', '000-321'],
            'flag_validinstall': [True, True, True]
        })
        df_contract = pd.DataFrame(data={
            "ContractNumber": [6827, 7783, 6539, 5767, 6857],
            "PDI_ContractType": ['new', 'new', 'existing', None, 'new'],
            "Service_Plan": ['gold', 'extended warranty', 'silver', 'gold', 'gold'],
            "Contract_Stage__c": ['warranty', 'closed', 'converted',
                                  'closed', 'warranty'],
            "StatusCode": ['activated', 'activated', 'draft', 'draft', 'activated'],
            "Original_Sales_Order__c": [2343, 4523, 6765, 6885, 3467],
            "PDI_Product_Family__c": ['pdu', 'static transfer switch',
                                      'rpp', 'pdu', 'pdu'],
            "was_startedup": [False, True, True, True, True],
            "startup_date": [None, '12/29/2020', '2/9/2000', '10/2/2010',
                             '8/10/2022'],
            "Start_Up_Completed_Date__c": [None, '12/29/2020', '2/9/2000',
                                           '10/2/2010', '8/10/2022'],
            "Customer_Tentative_Start_Up_Date__c": [None, None, None, None,
                                                    None],
            "Scheduled_Start_Up_Date__c": [None, None, None, None, None],
            "Warranty_Start_Date": [None, '12/29/2020', '2/9/2000', '10/2/2010',
                                    '8/10/2022'],
            "Warranty_Expiration_Date": [None, None, None, None, None],
            "Payment_Frequency": ['full amount', 'annual', 'full amount',
                                  'annual', 'annual'],
            "Start_date": [None, None, None, None, None],
            "BillingStreet": [None, None, None, None, None],
            "BillingCity": [None, None, None, None, None],
            "BillingState": [None, None, None, None, None],
            "BillingPostalCode": [None, None, None, None, None],
            "BillingCountry": [None, None, None, None, None],
            "BillingAddress": [None, None, None, None, None],
            "Country__c": [None, None, None, None, None],
            "Contract": [None, None, None, None, None],
            "Service_Sales_Manager": [None, None, None, None, None]})

        exp_res = pd.DataFrame(data={'ContractNumber': [6827, 7783, 6539],
                                     'Product': ['PDU', 'STS', 'RPP'],
                                     'SerialNumber': ['110-432-1', '118-221-7', '000-321'],
                                     "PDI_ContractType": ['new', 'new', 'existing'],
                                     "Service_Plan": ['gold', 'extended warranty', 'silver'],
                                     "Contract_Stage__c": ['warranty', 'closed', 'converted'],
                                     "StatusCode": ['activated', 'activated', 'draft'],
                                     "Original_Sales_Order__c": [2343, 4523, 6765],
                                     "PDI_Product_Family__c": ['pdu', 'static transfer switch',
                                                               'rpp'],
                                     "was_startedup": [False, True, True],
                                     "startup_date": [None, '12/29/2020', '2/9/2000'],
                                     "Start_Up_Completed_Date__c": [None, '12/29/2020', '2/9/2000'],
                                     "Customer_Tentative_Start_Up_Date__c": [None, None, None],
                                     "Scheduled_Start_Up_Date__c": [None, None, None],
                                     "Warranty_Start_Date": [None, '12/29/2020', '2/9/2000'],
                                     "Warranty_Expiration_Date": [None, None, None],
                                     "Payment_Frequency": ['full amount', 'annual', 'full amount'],
                                     "Start_date": [None, None, None],
                                     "BillingStreet": [None, None, None],
                                     "BillingCity": [None, None, None],
                                     "BillingState": [None, None, None],
                                     "BillingPostalCode": [None, None, None],
                                     "BillingCountry": [None, None, None],
                                     "BillingAddress": [None, None, None],
                                     "Country__c": [None, None, None],
                                     "Contract": [None, None, None],
                                     "Service_Sales_Manager": [None, None, None], })

        res = obj_contract.merge_contract_and_srnum(df_contract, df_contract_srnum)
        assert exp_res.equals(res)


class TestMergeContractAndRenewal:
    """
    Check if contract data and renewal data is getting merged correctly.
    """

    @pytest.mark.parametrize(
        "df_contract",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"PDI_ContractType": ['new', 'new', 'existing']})),
         ])
    def test_merge_contract_and_renewal_error1(self, df_contract):
        """
        Provided "df_contract" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        df_renewal = pd.DataFrame(data={"Contract_Status__c": ['closed',
                                                               'closed', 'closed'],
                                        "IsDeleted": [False, False, True],
                                        "Contract": ['80046000000cfVRAAY',
                                                     '80046000000cfVLAAY', '80046000000cfCIAAY']})
        with pytest.raises(Exception) as _:
            obj_contract.merge_contract_and_renewal(df_contract, df_renewal)

    @pytest.mark.parametrize(
        "df_renewal",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"Contract_Status__c": ['closed',
                                                    'closed', 'closed']})),
         ])
    def test_merge_contract_and_renewal_error2(self, df_renewal):
        """
        Provided "df_renewal" with
        None DataFrame
        Empty DataFrame
        string value
        list
        numeric value
        Missing Columns, should throw errors
        """
        df_contract = pd.DataFrame(data={"ContractNumber": [6827, 7783, 6539],
                                         "PDI_ContractType": ['new', 'new', 'existing'],
                                         "Service_Plan": ['gold', 'extended warranty', 'silver'],
                                         "Contract": ['80046000000cfVRAAY', '80046000000cfVLAAY',
                                                      '80046000000cfUIAAY']})
        with pytest.raises(Exception) as _:
            obj_contract.merge_contract_and_renewal(df_contract, df_renewal)

    def test_merge_contract_and_renewal_ideal_scenario(self):
        """
        Check if contract data and renewal data is getting merged correctly.
        """
        df_contract = pd.DataFrame(data={
            "ContractNumber": [6827, 7783, 6539],
            "PDI_ContractType": ['new', 'new', 'existing'],
            "Service_Plan": ['gold', 'extended warranty', 'silver'],
            "Contract": ['80046000000cfVRAAY', '80046000000cfVLAAY',
                      '80046000000cfUIAAY'],
            "Contract_Sales_Order__c": ["1", None, "3"],
            "Original_Sales_Order__c": ["2", "2", "4"],
            "BillingCustomer": ["0", "Only Customer", "1"],
            'BillingAddress': ["1", "6", "7"],
            'BillingCity': ["2", "7", "8"],
            'BillingState': ["3", "8", "9"],
            'BillingPostalCode': ["4", "9", "10"],
            'BillingCountry': ["5", "10", "11"]
        })
        df_renewal = pd.DataFrame(data={"Contract_Status__c": ['closed',
                                                               'closed'],
                                        "IsDeleted": [False, False],
                                        "Contract": ['80046000000cfVRAAY', '80046000000cfVLAAY']})

        ex_op1 = pd.DataFrame({
            "ContractNumber": [6827, 7783, 6539],
            "PDI_ContractType": ['new', 'new', 'existing'],
            "Service_Plan": ['gold', 'extended warranty', 'silver'],
            "Contract": ['80046000000cfVRAAY', '80046000000cfVLAAY',
                      '80046000000cfUIAAY'],
            "Contract_Sales_Order__c": ["1", None, "3"],
            "Original_Sales_Order__c": ["2", "2", "4"],
            "BillingCustomer": ["0", "Only Customer", "1"],
            'BillingAddress': ["1", "6", "7"],
            'BillingCity': ["2", "7", "8"],
            'BillingState': ["3", "8", "9"],
            'BillingPostalCode': ["4", "9", "10"],
            'BillingCountry': ["5", "10", "11"],
            "Contract_Status__c": ['closed', 'closed', None],
            "IsDeleted": [False, False, None]
        })
        df_contract = df_contract.merge(
            df_renewal, on='Contract', how='left')
        assert_frame_equal(df_contract, ex_op1)

        df_raw_m2m = pd.DataFrame({
            "SO": ["1", "2"],
            "Customer": ["a", None],
            'Sold to Street': ["b", "g"],
            'Sold to City': ["c", "h"],
            'Sold to State': ["d", "i"],
            'Sold to Zip': ["e", "j"],
            'Sold to Country': ["f", None]
        })

        df_contract = obj_contract.get_billto_data(df_contract, df_raw_m2m)
        # res = obj_contract.merge_contract_and_renewal(df_contract, df_renewal)
        print(df_contract)
        ex_op2 = pd.DataFrame({
            "ContractNumber": [6827, 7783, 6539],
            "PDI_ContractType": ['new', 'new', 'existing'],
            "Service_Plan": ['gold', 'extended warranty', 'silver'],
            "Contract": ['80046000000cfVRAAY', '80046000000cfVLAAY',
                         '80046000000cfUIAAY'],
            "Contract_Sales_Order__c": ["1", None, "3"],
            "Original_Sales_Order__c": ["2", "2", "4"],
            "BillingCustomer_old": ["0", "Only Customer", "1"],
            'BillingAddress_old': ["1", "6", "7"],
            'BillingCity_old': ["2", "7", "8"],
            'BillingState_old': ["3", "8", "9"],
            'BillingPostalCode_old': ["4", "9", "10"],
            'BillingCountry_old': ["5", "10", "11"],
            "Contract_Status__c": ['closed', 'closed', None],
            "IsDeleted": [False, False, None],
            "key_contract": ["1", "2", "3"],
            "key_SO": ["1", "2", None],
            "BillingCustomer": ["a", "Only Customer", "1"],
            'BillingAddress': ["b", "g", "7"],
            'BillingCity': ["c", "h", "8"],
            'BillingState': ["d", "i", "9"],
            'BillingPostalCode': ["e", "j", "10"],
            'BillingCountry': ["f", "10", "11"]
        })

        assert_frame_equal(df_contract, ex_op2)


class TestMergeContractAndInstall:
    """
    Check if contract and install base data are merged correctly.
    """

    @pytest.mark.parametrize(
        "install_df",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_merge_contract_install_err1(self, install_df):
        contract_data = {
            'SerialNumber': ['110-1667', '120-0036', '110-4033',
                             '110-3751', '411-0207'],
            'Warranty_Start_Date': [None, None, None, '10/14/2014',
                                    '10/14/2014'],
            'Warranty_Expiration_Date': [None, '12/31/2016',
                                         '1/10/2023', '7/24/2012',
                                         '10/13/2015'],
            'Contract_Start_Date': ['1/1/2021', '1/1/2015',
                                    None, None, '6/16/2020'],
            'Contract_Expiration_Date': ['12/31/2021', '12/31/2019', None,
                                         None, '6/15/2021']
        }
        contract_df = pd.DataFrame(contract_data)
        with pytest.raises(Exception) as _:
            result = obj_contract.merge_contract_install(contract_df, install_df)

    @pytest.mark.parametrize(
        "contract_df",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_merge_contract_install_err2(self, contract_df):
        install_data = {
            'SerialNumber': ['110-1667', '120-0036', '110-4033', '110-3751',
                             '411-0207'],
            'Product': ['Product A', 'Product B', 'Product C', 'Product D',
                        'Product E'],
            'Location': ['Location 1', 'Location 2', 'Location 3',
                         'Location 4', 'Location 5']
        }
        install_df = pd.DataFrame(install_data)
        with pytest.raises(Exception) as _:
            result = obj_contract.merge_contract_install(contract_df,
                                                         install_df)

    def test_merge_contract_install_ideal_scenario(self):
        contract_data = {
            'SerialNumber': ['110-1667', '120-0036', '110-4033',
                             '110-3751', '411-0207'],
            'Warranty_Start_Date': [None, None, None, '10/14/2014',
                                    '10/14/2014'],
            'Warranty_Expiration_Date': [None, '12/31/2016',
                                         '1/10/2023', '7/24/2012', '10/13/2015'],
            'Contract_Start_Date': ['1/1/2021', '1/1/2015',
                                    None, None, '6/16/2020'],
            'Contract_Expiration_Date': ['12/31/2021', '12/31/2019', None, None, '6/15/2021'],
            'was_startedup': [True, True, True, True, None]
        }

        install_data = {
            'SerialNumber': ['110-1667', '120-0036', '110-4033', '110-3751', '411-0207'],
            'Product': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
            'Location': ['Location 1', 'Location 2', 'Location 3', 'Location 4', 'Location 5']
        }

        expected_output = {'SerialNumber': ['110-1667', '120-0036', '110-4033', '110-3751', '411-0207'],
                           'Product': ['Product A', 'Product B', 'Product C', 'Product D',
                                       'Product E'],
                           'Location': ['Location 1', 'Location 2', 'Location 3', 'Location 4',
                                        'Location 5'],
                           'Warranty_Start_Date': [NaT, NaT, NaT, Timestamp('2014-10-14 00:00:00'),
                                                   Timestamp('2014-10-14 00:00:00')],
                           'Warranty_Expiration_Date': [NaT, Timestamp('2016-12-31 00:00:00'),
                                                        Timestamp('2023-01-10 00:00:00'),
                                                        Timestamp('2012-07-24 00:00:00'),
                                                        Timestamp('2015-10-13 00:00:00')],
                           'Contract_Start_Date': [Timestamp('2021-01-01 00:00:00'),
                                                   Timestamp('2015-01-01 00:00:00'), NaT, NaT,
                                                   Timestamp('2020-06-16 00:00:00')],
                           'Contract_Expiration_Date': [Timestamp('2021-12-31 00:00:00'),
                                                        Timestamp('2019-12-31 00:00:00'), NaT, NaT,
                                                        Timestamp('2021-06-15 00:00:00')],
                           'was_startedup': [True, True, True, True, False],
                           'First_Contract_Start_Date': [Timestamp('2021-01-01 00:00:00'),
                                                         Timestamp('2015-01-01 00:00:00'), NaT, NaT,
                                                         Timestamp('2020-06-16 00:00:00')],
                           'Contract_Conversion': ['No Warranty', 'Warranty Conversion',
                                                   'No Contract', 'No Contract', 'New Business']}

        contract_df = pd.DataFrame(contract_data)
        install_df = pd.DataFrame(install_data)
        expected_df = pd.DataFrame(expected_output)

        result = obj_contract.merge_contract_install(contract_df, install_df)

        print(result.to_dict(orient='list'))

        assert_frame_equal(result, expected_df, check_dtype=False, check_exact=False)

class TestGetBillToData:
    @pytest.mark.parametrize(
        "df_contract",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_get_bill_to_data_err1(self, df_contract):
        with pytest.raises(Exception) as _:
            df_contract = obj_contract.get_billto_data(df_contract)

    def test_get_bill_to_data_ideal_scenario(self):
        df_contract = pd.DataFrame({
            "Contract_Sales_Order__c": ["1", None],
            "Original_Sales_Order__c": ["2", "2"],
            "BillingCustomer": ["0", "Only Customer"],
            'BillingAddress': ["1", "6"],
            'BillingCity': ["2", "7"],
            'BillingState': ["3", "8"],
            'BillingPostalCode': ["4", "9"],
            'BillingCountry': ["5", "10"]
        })
        df_raw_m2m = pd.DataFrame({
            "SO": ["1", "2"],
            "Customer": ["a", None],
            'Sold to Street': ["b", "g"],
            'Sold to City': ["c", "h"],
            'Sold to State': ["d", "i"],
            'Sold to Zip': ["e", "j"],
            'Sold to Country': ["f", None]
        })

        ex_op = pd.DataFrame({
            "Contract_Sales_Order__c": ["1", None],
            "Original_Sales_Order__c": ["2", "2"],
            "BillingCustomer_old": ["0", "Only Customer"],
            'BillingAddress_old': ["1", "6"],
            'BillingCity_old': ["2", "7"],
            'BillingState_old': ["3", "8"],
            'BillingPostalCode_old': ["4", "9"],
            'BillingCountry_old': ["5", "10"],
            "key_contract": ["1", "2"],
            "key_SO": ["1", "2"],
            "BillingCustomer": ["a", "Only Customer"],
            'BillingAddress': ["b", "g"],
            'BillingCity': ["c", "h"],
            'BillingState': ["d", "i"],
            'BillingPostalCode': ["e", "j"],
            'BillingCountry': ["f", "10"]
        })
        df_contract = obj_contract.get_billto_data(df_contract, df_raw_m2m)
        assert_frame_equal(df_contract, ex_op, check_dtype=False, check_exact=False)
