"""@file test_services_data.py.

@brief This file used to test code for services data.




@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# Pytest execution command
# pytest ./tests/test_services_data.py
# pytest --cov=.\utils\dcpd --cov-report html:.\coverage\ .\tests\
# coverage report -m utils/dcpd/class_services_data.py


# !pytest ./tests/test_class_installbase.py
# !pytest --cov
# !pytest --cov=.\src --cov-report html:.\coverage\ .\test\
# !pytest --cov-report html:.\coverage\ .\test\
# !pytest --cov=.\src\class_services_data.py
# --cov-report html:.\coverage\ .\test\

# %%
from datetime import datetime, date, timedelta

import numpy as np
import pytest
import pandas as pd

from utils.filter_data import Filter
from utils.logger import AppLogger
from utils.dcpd import ProcessServiceIncidents
from utils.transform import Transform

logger = AppLogger('DCPD', level='')
filters_ = Filter()
obj_services = ProcessServiceIncidents()
from utils import IO


# %%
class TestServicesFunc:

    def test_pipeline_hardware_display_data(self):
        """
        Validate replace category output for given set of input params.
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(data = {'Id':'50046000000rCBdAAM', 'Customer_Issue_Summary__c':'Prescript display replace',
                                       'Customer_Issue__c':'PCBA - Software Issue','Resolution_Summary__c':'Done', 'Resolution__c':'Done',
                                       'Qty_1__c':'1','Qty_2__c':'0','Qty_3__c':'0'}, index=[0])

        expected_op = 'replace'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt,upgrade_component)
        actual_op = obj_result['type'][0]
        assert all([a == b for a, b in zip(actual_op, expected_op)])


    def test_pipeline_pdu_upgrade(self):
        """
        Validate pdu data for upgrade functionality.
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(data = {'Id':'50046000000rCBdAAJH', 'Customer_Issue_Summary__c':'Prescript m4 Postscript replace upgrade',
                                       'Customer_Issue__c':'Hardware Issue - PDU','Resolution_Summary__c':'SO 29095', 'Resolution__c':'Done',
                                       'Qty_1__c':'1','Qty_2__c':'3','Qty_3__c':'4'}, index=[0])

        expected_op_1 = 'upgrade'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt,upgrade_component)
        actual_op_1 = obj_result['type'][0]

        expected_op_2 = 'Display'
        obj_result_1 = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt,upgrade_component)
        actual_op_2 = obj_result_1['component'][0]
        errors = []

        # replace assertions by conditions
        if not (expected_op_1 == actual_op_1):
            assert all([a == b for a, b in zip(expected_op_1, actual_op_1)])
            errors.append("No replace type present")

        if not (expected_op_2 == actual_op_2):
            assert all([a == b for a, b in zip(expected_op_2, actual_op_2)])
            errors.append("No display component present")

        # assert no error message has been registered, else print messages
        assert not errors, "errors occured:\n{}".format("\n".join(errors))

    def test_pipeline_bcms_replace(self):
        """
        Validate BCMS component for replace functionality
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(
            data={'Id': '50046000000rCBdAAJH', 'Customer_Issue_Summary__c': 'BCMS replace',
                  'Customer_Issue__c': 'PCBA', 'Resolution_Summary__c': 'SO 29095',
                  'Resolution__c': 'Done',
                  'Qty_1__c': '1', 'Qty_2__c': '0', 'Qty_3__c': '0'}, index=[0])

        expected_op_1 = 'replace'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt, upgrade_component)
        actual_op_1 = obj_result['type'][0]

        expected_op_2 = 'BCMS'
        obj_result_1 = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt, upgrade_component)
        actual_op_2 = obj_result_1['component'][0]
        errors = []

        # replace assertions by conditions
        if not (expected_op_1 == actual_op_1):
            assert all([a == b for a, b in zip(expected_op_1, actual_op_1)])
            errors.append("No replace type present")

        if not (expected_op_2 == actual_op_2):
            assert all([a == b for a, b in zip(expected_op_2, actual_op_2)])
            errors.append("No display component present")

        # assert no error message has been registered, else print messages
        assert not errors, "errors occured:\n{}".format("\n".join(errors))


    def test_pipeline__hardware_rpp_replace(self):
        """
        Validate RPP component type for replace type
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(
            data={'Id': '50046000000rCBdAAJH', 'Customer_Issue_Summary__c': 'BCMS replace',
                  'Customer_Issue__c': 'Hardware Issue - RPP', 'Resolution_Summary__c': 'BO -9095',
                  'Resolution__c': 'Done',
                  'Qty_1__c': '1', 'Qty_2__c': '0', 'Qty_3__c': '0'}, index=[0])

        expected_op_1 = 'replace'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt, upgrade_component)
        actual_op_1 = obj_result['type'][0]

        expected_op_2 = 'BCMS'
        obj_result_1 = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt, upgrade_component)
        actual_op_2 = obj_result_1['component'][0]
        errors = []

        # replace assertions by conditions
        if not (expected_op_1 == actual_op_1):
            assert all([a == b for a, b in zip(expected_op_1, actual_op_1)])
            errors.append("No replace type present")

        if not (expected_op_2 == actual_op_2):
            assert all([a == b for a, b in zip(expected_op_2, actual_op_2)])
            errors.append("No display component present")

        # assert no error message has been registered, else print messages
        assert not errors, "errors occured:\n{}".format("\n".join(errors))

    def test_pipline_init(self):
        """
        Validate if init function is invoked as parameters do not throw exception
        """
        with pytest.raises(Exception) as info:
            obj_services.__init__(self)
            assert info.type == Exception

    def test_pipline_main(self):
        """
        Test the main pipline if executing with the testdata
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})

        file_dir = {'file_dir': self.config['file']['dir_data'],
                    'file_name': self.config['file']['Raw']
                    ['services']['file_name']}
        df_services_raw = IO.read_csv('local', file_dir)

        with pytest.raises(Exception) as info:
            obj_services.main_services(self)
            assert info.type == Exception

    def test_pipline_jcomm(self):
        """
        Validate functioning of Jcomm and Sidecar data fields with raw data
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})

        file_dir = {'file_dir': self.config['file']['dir_data'],
                    'file_name': self.config['file']['Raw']
                    ['services']['file_name']}
        df_services_raw = file_dir

        with pytest.raises(Exception) as info:
            obj_services.pipline_component_identify(self)
            assert info.type == Exception

    def test_pipline_jcomm_output_null(self):
        """
        Validate Jcomm and Sidecar data fields for empty data.
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})

        file_dir = {'file_dir': self.config['file']['dir_data'],
                    'file_name': self.config['file']['Raw']
                    ['services']['file_name']}
        df_services_raw = file_dir

        df_jcomm_output = pd.DataFrame()

        with pytest.raises(Exception) as info:
            obj_services.pipline_component_identify(self,df_services_raw)
            assert info.type == Exception

# Testcases for extracting raw serial number data
# pipeline_serial_number
    def test_pipeline_serial_number(self):
        """
        Validate if pipline serial number validates empty df
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['SerialNumberColumns']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        file_dir = {'file_dir': self.config['file']['dir_data'],
                    'file_name': self.config['file']['Raw']
                    ['services']['file_name']}
        df_services_raw = file_dir

        data = {'Id': '50046000000rC53AAE',
                'Customer_Issue_Summary__c': 'a PDU unit that we just started up that the display is no longer working;'
                                             ' 110-4161'}
        df_srNum = pd.DataFrame()
        expected_op = ''

        with pytest.raises(Exception) as info:
            actual_op = obj_services.pipeline_serial_number(df_srNum,dict_filt)
            assert info.type == Exception

    def test_pipeline_hardware_changes_empty_df(self):
        """
        Validate hardware changes data with empty dataframe.
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(data = {'Id':'Test_123', 'Customer_Issue_Summary__c':'Prescript display replace',
                                       'Customer_Issue__c':'PCBA - Software Issue','Resolution_Summary__c':'Done', 'Resolution__c':'Done',
                                       'Qty_1__c':'1','Qty_2__c':'0','Qty_3__c':'0'}, index=[0])

        file_dir = {'file_dir': self.config['file']['dir_data'],
                    'file_name': self.config['file']['Raw']
                    ['services']['file_name']}
        df_services_raw = file_dir

        # Creating an empty dataframe
        df_srNum = pd.DataFrame()
        expected_op = ''

        with pytest.raises(Exception) as info:
            actual_op = obj_services.pipeline_id_hardwarechanges(df_srNum, dict_filt,upgrade_component)
            assert info.type == ValueError

    def test_pipeline_hardware_ideal_data(self):
        """
        Validate hardware changes data for ideal case scenario
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(data = {'Id':'Test_123', 'Customer_Issue_Summary__c':'Prescript display replace',
                                       'Customer_Issue__c':'PCBA - Software Issue','Resolution_Summary__c':'Done', 'Resolution__c':'Done',
                                       'Qty_1__c':'1','Qty_2__c':'0','Qty_3__c':'0'}, index=[0])

        expected_op = 'Display'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt,upgrade_component)
        actual_op = obj_result['component'][0]
        assert all([a == b for a, b in zip(actual_op, expected_op)])

    def test_pipeline_ideal_rep_data(self):
        """
        Validate hardware changes output with two conditions for display and replace functionality
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(data = {'Id':'50046000000rCBdAAJH', 'Customer_Issue_Summary__c':'Prescript display replace',
                                       'Customer_Issue__c':'Installation Request','Resolution_Summary__c':'Done', 'Resolution__c':'Done',
                                       'Qty_1__c':'1','Qty_2__c':'0','Qty_3__c':'0'}, index=[0])

        expected_op_1 = 'replace'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt,upgrade_component)
        actual_op_1 = obj_result['type'][0]

        expected_op_2 = 'Display'
        obj_result_1 = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt,upgrade_component)
        actual_op_2 = obj_result_1['component'][0]
        errors = []

        # replace assertions by conditions
        if not (expected_op_1 == actual_op_1):
            assert all([a == b for a, b in zip(expected_op_1, actual_op_1)])
            errors.append("No replace type present")
        if not (expected_op_2 == actual_op_2):
            assert all([a == b for a, b in zip(expected_op_2, actual_op_2)])
            errors.append("No display component present")

        # assert no error message has been registered, else print messages
        assert not errors, "errors occured:\n{}".format("\n".join(errors))

    # @pytest.mark.parametrize(
    #     "hardware_changes",
    #     [
    #         '5004600000', 'A2GHqAAN	"customer received the display and installed it.  However, display '
    #                       'was not working properly.We had a technician troubleshoot over the phone.  '
    #                       'it was decided to send a replacement display site Upsistemas Brazil	',
    #         'PCBA - Hardware Issue,	"replacement board shipped 6/26 UPS 1z2698840352736805',
    #         'Hardware/Parts Provided', 'Display', 'replace'
    #     ],
    # )
    # def test_services_merge_data(self,hardware_changes):
    #     """
    # Validate merge functionality of dataset
    #     """
    #     # Map the column value to the data value.
    #     hardwareDf = pd.DataFrame(data = {''})
    #     hardware_changes_ = hardware_changes
    #     df_services_raw = hardware_changes_
    #     df_sr_num = hardware_changes_
    #     expected_output = hardware_changes_
    #     actual_output = obj_services.merge_data(hardware_changes,df_services_raw,df_sr_num)
    #     # with pytest.raises(Exception) as info:
    #         # obj_services.merge_data(self,hardware_changes,df_services_raw,df_sr_num)
    #     assert all([a == b for a, b in zip(actual_output, expected_output)])
    #


    # @pytest.mark.parametrize(
    #     "Id, Customer_Issue_Summary__c,Customer_Issue__c,Resolution_Summary__c,Resolution__c,Qty_1__c,Qty_2__c,Qty_3__c",
    #     [
    #         (['Test_123'], ['Prescript display replace'],['PCBA - Software Issue'],['Done'],['Done'],[1],[0],[0])
    #     ],
    # )
    # def test_pipeline_hardware_merge_data(self,Id,Customer_Issue_Summary__c,Customer_Issue__c,Resolution_Summary__c,Resolution__c,Qty_1__c,Qty_2__c,Qty_3__c):
    #     """
    #     """
    #     self.config = IO.read_json(mode='local', config={
    #         "file_dir": './references/', "file_name": 'config_dcpd.json'})
    #     dict_filt = self.config['services']['Component_replacement']
    #     upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']
    #
    #     # df_data = pd.DataFrame({Id,Customer_Issue_Summary__c,Customer_Issue__c,Resolution_Summary__c,Resolution__c,Qty_1__c,Qty_2__c,Qty_3__c})
    #     set_df_data = set([Id,Customer_Issue_Summary__c,Customer_Issue__c,Resolution_Summary__c,Resolution__c,Qty_1__c,Qty_2__c,Qty_3__c])
    #     df_data = pd.DataFrame(set_df_data)
    #     expected_op = 'replace'
    #     obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt,upgrade_component)
    #     actual_op = obj_result['type'][0]
    #     assert all([a == b for a, b in zip(actual_op, expected_op)])

# %%
if __name__ == "__main__":
    testclass = TestServicesFunc()
    testclass.test_main_func()
