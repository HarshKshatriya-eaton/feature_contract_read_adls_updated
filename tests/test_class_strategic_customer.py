"""@file test_class_generate_contact_data.py.

@brief This file used to test code for strategic customer identification which uses the output
from class_generate_contacts and output of class_installbase



@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

import pytest
import pandas as pd
from datetime import datetime
from pandas._testing import assert_frame_equal
from utils.logger import AppLogger


logger = AppLogger('DCPD', level='')
from utils import IO
import numpy as np
from utils.strategic_customer import StrategicCustomer

obj_strategic_customer = StrategicCustomer()


class TestIdentification:
    """
    Test for the strategic customer identification from strategic_customer.py
    inputs are:
    df_leads: class_installbase output
    df_contacts: class_generate_contacts output
    df_ref: new ref identification
    """
    #
    def test_pipline_init(self):
        """
        Validate if init function is invoked as parameters do not throw exception
        """

        with pytest.raises(Exception) as info:
            obj_strategic_customer.__init__(self)
            assert info.type == Exception


    def test_read_ref_data(self):
        """
        Check if the formating of ref data is done from strategic_customer1.py
        """

        input_data = [{'DisplayName': 'DisplayName', ' Condition 1': 'MatchType_00', ' ': 'CompanyName', ' Condition 2': 'MatchType_01', ' .1': 'CompanyAliasName', ' Condition 3': 'MatchType_02', ' .2': 'CompanyDomain'}, {'DisplayName': 'ABB', ' Condition 1': 'begins with', ' ': 'ABB;Zenith', ' Condition 2': 'begins with', ' .1': 'ABB;Zenith', ' Condition 3': 'ends with', ' .2': 'abb.com'}, {'DisplayName': 'Aligned Energy, LLC', ' Condition 1': 'begins with', ' ': 'Aligned Energy, LLC;Align;Intertech', ' Condition 2': 'begins with', ' .1': 'Aligned Energy, LLC;Align;Intertech', ' Condition 3': 'ends with', ' .2': 'aligneddc.com'}]

        ref_ac_manager = pd.DataFrame(input_data)
        result = obj_strategic_customer.read_ref_data(ref_ac_manager)

        expected_output = pd.DataFrame([{0: '1', 'DisplayName': 'abb', 'MatchType_00': 'begins with', 'CompanyName': 'abb;zenith', 'MatchType_01': 'begins with', 'CompanyAliasName': 'abb;zenith', 'MatchType_02': 'ends with', 'CompanyDomain': 'abb.com'}, {0: '2', 'DisplayName': 'aligned energy, llc', 'MatchType_00': 'begins with', 'CompanyName': 'aligned energy, llc;align;intertech', 'MatchType_01': 'begins with', 'CompanyAliasName': 'aligned energy, llc;align;intertech', 'MatchType_02': 'ends with', 'CompanyDomain': 'aligneddc.com'}])

        assert np.array_equal(result.values,expected_output.values)


    def test_read_processed_m2m_data(self):
        """
        Check if the processed install data is read to extract ship_to_customer
            so that it can be compared with company alias name
        """

        input_data = [{'Customer': 'wright line, llc', 'ShipTo_Customer': 'e on u s services inc', 'SerialNumber_M2M': '185-0043-co'}, {'Customer': 'cupertino electric, inc.', 'ShipTo_Customer': 'apple lazaneo', 'SerialNumber_M2M': '110-2768'}, {'Customer': 'qts', 'ShipTo_Customer': 'schneider electric', 'SerialNumber_M2M': 't18-26-us-s-4313'}]
        df_leads = pd.DataFrame(input_data)
        result=obj_strategic_customer.read_processed_m2m(df_leads)

        expected_output = pd.DataFrame([{'Serial_Number': '110-2768', 'CompanyName': 'cupertino electric, inc', 'CompanyAliasName': 'apple lazaneo'}, {'Serial_Number': 't18-26-us-s-4313', 'CompanyName': 'qts', 'CompanyAliasName': 'schneider electric'}, {'Serial_Number': '185-0043-co', 'CompanyName': 'wright line, llc', 'CompanyAliasName': 'e on u s services inc'}])

        assert np.array_equal(result.values,expected_output.values)

    def test_read_contact(self):
        """
        Check if the processed contacts data is read to extract email
            so that it can be compared with company domanin
        """
        input_data =[{'Serial_Number': '110-2798-87', 'Email__c': 'michael_balk@optum.com'}, {'Serial_Number': '411-0062', 'Email__c': 'mmcnulty@drwholdings.com'}, {'Serial_Number': '110-2993-27', 'Email__c': 'larry.scharp@fortunedatacenters.com'}]
        df_contact = pd.DataFrame(input_data)

        result = obj_strategic_customer.read_contact(df_contact)

        expected_output = pd.DataFrame([{'Serial_Number': '110-2798-87', 'Email': 'michael_balk@optum.com'}, {'Serial_Number': '411-0062', 'Email': 'mmcnulty@drwholdings.com'}, {'Serial_Number': '110-2993-27', 'Email': 'larry.scharp@fortunedatacenters.com'}])

        assert_frame_equal(result.sort_index(axis=1), expected_output.sort_index(axis=1),
                           check_dtype=False, check_exact=False, check_names=True)


    def test_pipeline_identify_customers(self):
        """
        Testing customer identification logic for all strategic customers.
        df_contact: passed input from contacts data and invoked read contact function for required formating
        df_leads: passed input from processed_install and invoked processed_m2m function

        """
        input_data_contact = [{'Serial_Number': '110-2798-87', 'Email__c': 'michael_balk@optum.com'},
                      {'Serial_Number': '411-0062', 'Email__c': 'mmcnulty@drwholdings.com'},
                      {'Serial_Number': '110-2993-27', 'Email__c': 'larry.scharp@fortunedatacenters.com'},
                      {'Serial_Number': '111-2222-33', 'Email__c': 'test_customer@test1.com'},
                      {'Serial_Number': '111-2222-34', 'Email__c': 'test_customer@test2.com'}
                      ]
        df_contact = pd.DataFrame(input_data_contact)
        df_contact = obj_strategic_customer.read_contact(df_contact)

        input_data_leads = [{'Customer': 'parsons electric', 'ShipTo_Customer': 'unitedhealth group', 'SerialNumber_M2M': '110-2798-87'}, {'Customer': 'cupertino electric, inc.', 'ShipTo_Customer': 'apple lazaneo', 'SerialNumber_M2M': '110-2768'}, {'Customer': 'qts', 'ShipTo_Customer': 'schneider electric', 'SerialNumber_M2M': 't18-26-us-s-4313'}, {'Customer': 'test1', 'ShipTo_Customer': 'test1-energy-providers', 'SerialNumber_M2M': '111-2222-33'}, {'Customer': 'test1', 'ShipTo_Customer': 'power electric/test1-energy-providers', 'SerialNumber_M2M': '111-2222-33'}, {'Customer': 'test2', 'ShipTo_Customer': 'test2-energy-providers', 'SerialNumber_M2M': '111-2222-34'}, {'Customer': 'test2', 'ShipTo_Customer': 'power electric/test2-energy-providers', 'SerialNumber_M2M': '111-2222-34'}]
        df_leads = pd.DataFrame(input_data_leads)
        df_leads= obj_strategic_customer.read_processed_m2m(df_leads)
        df_leads = obj_strategic_customer.summarize_contacts(df_contact, df_leads)

        ref_data = [{'DisplayName': 'DisplayName', ' Condition 1': 'MatchType_00', ' ': 'CompanyName', ' Condition 2': 'MatchType_01', ' .1': 'CompanyAliasName', ' Condition 3': 'MatchType_02', ' .2': 'CompanyDomain'}, {'DisplayName': 'ABB', ' Condition 1': 'begins with', ' ': 'ABB;Zenith', ' Condition 2': 'begins with', ' .1': 'ABB;Zenith', ' Condition 3': 'ends with', ' .2': 'abb.com'}, {'DisplayName': 'Aligned Energy, LLC', ' Condition 1': 'begins with', ' ': 'Aligned Energy, LLC;Align;Intertech', ' Condition 2': 'begins with', ' .1': 'Aligned Energy, LLC;Align;Intertech', ' Condition 3': 'ends with', ' .2': 'aligneddc.com'}, {'DisplayName': 'test1', ' Condition 1': 'begins with', ' ': 'random_val', ' Condition 2': 'begins with', ' .1': 'test1', ' Condition 3': 'ends with', ' .2': 'random.com'}, {'DisplayName': 'test2', ' Condition 1': 'contains', ' ': 'random_val', ' Condition 2': 'contains', ' .1': 'test2', ' Condition 3': 'ends with', ' .2': 'random.com'}]
        ref_ac_manager = pd.DataFrame(ref_data)
        ref_df = obj_strategic_customer.read_ref_data(ref_ac_manager)
        result = obj_strategic_customer.pipeline_identify_customers(ref_df, df_leads)
        expected_output = pd.DataFrame([
            {'Serial_Number': '111-2222-33',
             'CompanyName': 'test1',
             'CompanyAliasName': 'test1-energy-providers',
             'CompanyDomain': 'test_customer@test1.com',
             'StrategicCustomer': 'test1',
             'StrategicCustomer_new': 'test1'},
            {'Serial_Number': '111-2222-34',
             'CompanyName': 'test2',
             'CompanyAliasName': 'power electric/test2-energy-providers',
             'CompanyDomain': 'test_customer@test2.com',
             'StrategicCustomer': 'test2',
             'StrategicCustomer_new': 'test2'},
            {'Serial_Number': '111-2222-34',
             'CompanyName': 'test2',
             'CompanyAliasName': 'test2-energy-providers',
             'CompanyDomain': 'test_customer@test2.com',
             'StrategicCustomer': 'test2',
             'StrategicCustomer_new': 'test2'},
            {'Serial_Number': '110-2768',
             'CompanyName': 'cupertino electric, inc',
             'CompanyAliasName': 'apple lazaneo', 'CompanyDomain': np.nan,
             'StrategicCustomer': 'Other',
             'StrategicCustomer_new': 'cupertino electric, inc'},
            {'Serial_Number': '110-2798-87', 'CompanyName': 'parsons electric',
             'CompanyAliasName': 'unitedhealth group',
             'CompanyDomain': 'michael_balk@optum.com',
             'StrategicCustomer': 'Other',
             'StrategicCustomer_new': 'parsons electric'},
            {'Serial_Number': 't18-26-us-s-4313', 'CompanyName': 'qts',
             'CompanyAliasName': 'schneider electric', 'CompanyDomain': np.nan,
             'StrategicCustomer': 'Other', 'StrategicCustomer_new': 'qts'},
            {'Serial_Number': '111-2222-33',
             'CompanyName': 'test1',
             'CompanyAliasName': 'power electric/test1-energy-providers',
             'CompanyDomain': 'test_customer@test1.com',
             'StrategicCustomer': 'Other',
             'StrategicCustomer_new': 'test1'}
            ]
        )

        result = result.reset_index()

        expected_output = expected_output.reset_index()
        result = result.drop("index", axis=1)
        expected_output = expected_output.drop("index", axis=1)
        assert_frame_equal(result.sort_index(axis=1), expected_output.sort_index(axis=1),
                           check_dtype=False, check_exact=False, check_names=True)

        with pytest.raises(Exception) as info:
            obj_strategic_customer.pipeline_identify_customers()
            assert info.type == Exception



# %% *** Call ***

if __name__ == "__main__":
    obj_strategic = TestIdentification()
