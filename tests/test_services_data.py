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
# coverage run -m pytest ./tests/test_class_services_data.py
# coverage report -m

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
from pandas._testing import assert_frame_equal

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

        df_data = pd.DataFrame(
            data={'Id': '50046000000rCBdAAM', 'Customer_Issue_Summary__c': 'Prescript display replace',
                  'Customer_Issue__c': 'PCBA - Software Issue', 'Resolution_Summary__c': 'Done',
                  'Resolution__c': 'Done',
                  'Qty_1__c': '1', 'Qty_2__c': '0', 'Qty_3__c': '0'}, index=[0])

        expected_op = 'replace'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt, upgrade_component)
        actual_op = obj_result['type'][0]
        assert all([a == b for a, b in zip(actual_op, expected_op)])

    def test_pipeline_hardware_display_data_caseSensitive(self):
        """
        Validate replace category output for given set of input params.
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(
            data={'Id': '50046000000rCBdAAM', 'Customer_Issue_Summary__c': 'Prescript display REPLACE',
                  'Customer_Issue__c': 'PCBA - Software Issue', 'Resolution_Summary__c': 'Done',
                  'Resolution__c': 'Done',
                  'Qty_1__c': '1', 'Qty_2__c': '0', 'Qty_3__c': '0'}, index=[0])
        # Testing done for REPLACE, REPLACED, Replace and replace.
        expected_op = 'replace'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt, upgrade_component)
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

        df_data = pd.DataFrame(
            data={'Id': '50046000000rCBdAAJH', 'Customer_Issue_Summary__c': 'Prescript m4 Postscript replace upgrade',
                  'Customer_Issue__c': 'Hardware Issue - PDU', 'Resolution_Summary__c': 'SO 29095',
                  'Resolution__c': 'Done',
                  'Qty_1__c': '1', 'Qty_2__c': '3', 'Qty_3__c': '4'}, index=[0])

        expected_op_1 = 'upgrade'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt, upgrade_component)
        actual_op_1 = obj_result['type'][0]

        expected_op_2 = 'Display'
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

        with pytest.raises(Exception) as info:
            obj_services.main_services(self)
            assert info.type == Exception

    def test_pipline_jcomm(self):
        """
        Validate functioning of Jcomm and Sidecar data fields with raw data
        """
        with pytest.raises(Exception) as info:
            obj_services.pipline_component_identify(self)
            assert info.type == Exception

    def test_pipline_jcomm_output_null(self):
        """
        Validate Jcomm and Sidecar data fields for empty data.
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})

        df_jcomm_output = pd.DataFrame()

        with pytest.raises(Exception) as info:
            obj_services.pipline_component_identify(self, df_jcomm_output,df_jcomm_output)
            assert info.type == Exception

    def test_pipeline_jcomm_valid(self):
        """
        Validate Jcomm and Sidecar data fields for ideal data.
        """

        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        self.mode = 'local'
        file_dir = {'file_dir': self.config['file']['dir_data'],
                    'file_name': self.config['file']['Raw']
                    ['services']['file_name']}
        df_services_raw = IO.read_csv(self.mode, file_dir)
        # df_services_raw.head()
        test = ''

        file_dir = {'file_dir': self.config['file']['dir_intermediate'],
                    'file_name': self.config['file']['Processed']['services']['serial_number_services']}
        df_serial = IO.read_csv(self.mode, file_dir)
        # df_serial.head(50)

        # df_temp = df_services_raw.iloc[:1]
        # df_updated = df_temp.to_dict(orient='list')
        # df_new = pd.DataFrame.from_dict(df_updated)
        # # df['SerialNumber'] = df['column name'].replace(['old value'], 'new value')
        # print(df_new)
        # # data =

        data = {'Id': 'Test1234563121', 'Customer_Issue_Summary__c': 'Testing JCOMM SrNo: 110-2334','Product_1__c':'1','Qty_1__c':1,'KeySerial':'10210:45'}
        df_services_raw = pd.DataFrame(data, index=[0])

        data_srnum = pd.DataFrame({'Id': 'Test1234563121','SerialNumber': '110-2334','Qty':1,'KeySerial':'10210:45','src':'Product_1__c'},index=[0])

        # data_srnum = pd.DataFrame({'SerialNumber': '180-1854', 'Id': '50046000003p5gwAAA', 'SerialNumberContract': '180-1854', 'Qty': 6,
        #         'src': 'Product_1__c'},index=[0])

        dict_cols_srnum = self.config['services']['SerialNumberColumns']
        with pytest.raises(Exception) as info:
            obj_services.pipline_component_identify(df_services_raw, data_srnum)
            assert info == Exception

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
            actual_op = obj_services.pipeline_serial_number(df_srNum, dict_filt)
            assert info.type == Exception

    def test_pipeline_hardware_changes_empty_df(self):
        """
        Validate hardware changes data with empty dataframe.
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(data={'Id': 'Test_123', 'Customer_Issue_Summary__c': 'Prescript display replace',
                                     'Customer_Issue__c': 'PCBA - Software Issue', 'Resolution_Summary__c': 'Done',
                                     'Resolution__c': 'Done',
                                     'Qty_1__c': '1', 'Qty_2__c': '0', 'Qty_3__c': '0'}, index=[0])

        file_dir = {'file_dir': self.config['file']['dir_data'],
                    'file_name': self.config['file']['Raw']
                    ['services']['file_name']}
        df_services_raw = file_dir

        # Creating an empty dataframe
        df_srNum = pd.DataFrame()
        expected_op = ''

        with pytest.raises(Exception) as info:
            actual_op = obj_services.pipeline_id_hardwarechanges(df_srNum, dict_filt, upgrade_component)
            assert info.type == ValueError

    def test_pipeline_hardware_ideal_data(self):
        """
        Validate hardware changes data for ideal case scenario
        """
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_filt = self.config['services']['Component_replacement']
        upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']

        df_data = pd.DataFrame(data={'Id': 'Test_123', 'Customer_Issue_Summary__c': 'Prescript display replace',
                                     'Customer_Issue__c': 'PCBA - Software Issue', 'Resolution_Summary__c': 'Done',
                                     'Resolution__c': 'Done',
                                     'Qty_1__c': '1', 'Qty_2__c': '0', 'Qty_3__c': '0'}, index=[0])

        expected_op = 'Display'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt, upgrade_component)
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

        df_data = pd.DataFrame(
            data={'Id': '50046000000rCBdAAJH', 'Customer_Issue_Summary__c': 'Prescript display replace',
                  'Customer_Issue__c': 'Installation Request', 'Resolution_Summary__c': 'Done', 'Resolution__c': 'Done',
                  'Qty_1__c': '1', 'Qty_2__c': '0', 'Qty_3__c': '0'}, index=[0])

        expected_op_1 = 'replace'
        obj_result = obj_services.pipeline_id_hardwarechanges(df_data, dict_filt, upgrade_component)
        actual_op_1 = obj_result['type'][0]

        expected_op_2 = 'Display'
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

    @pytest.mark.parametrize(
        "df_org",
        [None,
         (pd.DataFrame())
         ], )
    def test_pipeline_serial_empty(self, df_org):

        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        dict_cols_srnum = self.config['services']['SerialNumberColumns']
        with pytest.raises(Exception) as _:
            obj_services.pipeline_serial_number(df_org)

    def test_pipeline_serial_ideal_data(self):

        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        self.mode = 'local'
        file_dir = {'file_dir': self.config['file']['dir_data'],
                    'file_name': self.config['file']['Raw']
                    ['services']['file_name']}
        df_services_raw = IO.read_csv(self.mode, file_dir)
        df_services_raw.head(5)

        dict_cols_srnum = self.config['services']['SerialNumberColumns']
        with pytest.raises(Exception) as info:
            obj_services.pipeline_serial_number(df_services_raw, dict_cols_srnum)
            assert info == Exception


    def test_jcomm_sidecar_test_input(self):

        raw_data =  [{'Id': '50046000003p5gwAAA', 'IsDeleted': False, 'MasterRecordId': np.nan, 'CaseNumber': 18491, 'ContactId': '0034600000x4830AAA', 'AccountId': '0014600000ihZuLAAU', 'Type': 'Time & Material', 'RecordTypeId': '01246000000H2ESAA0', 'Status': 'Closed', 'Reason': np.nan, 'Language': np.nan, 'Subject': np.nan, 'Priority': 'Medium', 'Description': np.nan, 'IsClosed': True, 'ClosedDate': '8/5/2018 0:28', 'OwnerId': '00546000000toGPAAY', 'CreatedDate': '4/30/2018 19:37', 'CreatedById': '00546000000toGPAAY', 'LastModifiedDate': '8/5/2018 0:28', 'LastModifiedById': '00546000000toGPAAY', 'SystemModstamp': '8/5/2018 0:28', 'ContactPhone': 1, 'ContactMobile': 1, 'ContactEmail': 'Blank', 'ContactFax': np.nan, 'Comments': np.nan, 'LastViewedDate': np.nan, 'LastReferencedDate': np.nan, 'Account_Name__c': np.nan, 'Address__c': ', , , -', 'Billing_Account_Name__c': np.nan, 'Billing_City__c': np.nan, 'Billing_Contact__c': 'Blank', 'Billing_Country__c': np.nan, 'Billing_Customer__c': np.nan, 'Billing_State__c': np.nan, 'Billing_Street__c': np.nan, 'Billing_Zip_Code_Postal_Code__c': np.nan, 'Business_Unit_Mfg_Site__c': 'SPDI', 'Business_Unit__c': 'SPDI', 'CAR_Action__c': np.nan, 'CAR_Aging__c': np.nan, 'CAR_Status__c': np.nan, 'City__c': np.nan, 'Closed_Date__c': np.nan, 'Company_Name__c': np.nan, 'Company__c': np.nan, 'Completion_Status__c': np.nan, 'Contact_Email1__c': 'Blank', 'Contact_Email__c': 'Blank', 'Contact_Name1__c': 'Blank', 'Contact_Name_lookup__c': np.nan, 'Contact_Phone1__c': 1, 'Contact_Phone__c': 1, 'Contact_Title__c': np.nan, 'Containment_Date__c': np.nan, 'Contract_Sales_Order__c': np.nan, 'Contract__c': np.nan, 'Corrective_Action_Close_Date__c': np.nan, 'Corrective_Action_Comments__c': np.nan, 'Corrective_Action_Number__c': np.nan, 'Corrective_Action_Start_Date__c': np.nan, 'Corrective_Action__c': np.nan, 'Corrective_Date__c': np.nan, 'Cost_of_Poor_Quality_CoPQ__c': np.nan, 'Country__c': np.nan, 'Customer_Concern_Metric__c': np.nan, 'Customer_Issue_Summary__c': 'heat mitigation retrofit kit 10 PDUs  110-2334 Each kit includes: FIELD RETROFIT KIT NO SIDECAR Each Kit Includes:', 'Customer_Issue__c': 'Hardware Issue - Wiring Problem', 'Date_CAR_Status_changed_from_In_CAR__c': np.nan, 'Date_CAR_Status_changed_from_In_Process__c': np.nan, 'Date_CAR_Status_changed_from_Open__c': np.nan, 'Date_CAR_Status_set_to_In_CAR__c': np.nan, 'Date_CAR_Status_set_to_In_Process__c': np.nan, 'Date_CAR_Status_set_to_Open__c': np.nan, 'Date_of_Response__c': np.nan, 'Days_In_CAR__c': np.nan, 'Days_In_Process__c': np.nan, 'Days_in_open__c': np.nan, 'Days_to_Response__c': np.nan, 'Email__c': np.nan, 'FirstName__c': 'Blank', 'Frequency__c': np.nan, 'Identified_Department_Owner__c': np.nan, 'Identified_Department__c': np.nan, 'In_Process_Projected_Close_Date__c': np.nan, 'In_Process_Start_Date__c': np.nan, 'Internal_Defect_Code__c': np.nan, 'Investigation_Summary__c': np.nan, 'M2M_RMA__c': np.nan, 'Next_Update__c': np.nan, 'Notify_Service_SPDI__c': False, 'Opportunity__c': np.nan, 'Original_Sales_Order_del__c': np.nan, 'PDI_Cost__c': np.nan, 'Phone__c': np.nan, 'Preventative_Date__c': np.nan, 'Product_1__c': '110-2334', 'Product_2__c': np.nan, 'Product_3__c': np.nan, 'Product_Safety__c': np.nan, 'Product_Type_SPDI__c': np.nan, 'Production_Supervisor_SPWR__c': np.nan, 'Productivity_SPWR__c': np.nan, 'Progress_Update__c': np.nan, 'Qty_1__c': 4, 'Qty_2__c': np.nan, 'Qty_3__c': np.nan, 'Quality_Engineer_SPWR__c': np.nan, 'Quality_Incident_Report__c': False, 'Quality_Technician_SPWR__c': np.nan, 'Resolution_Summary__c': np.nan, 'Resolution__c': np.nan, 'Resolved_Date__c': np.nan, 'PDI_Product_Family__c': 'PDU', 'Sales_Person__c': '00546000000toHVAAY', 'Serial_Date_Lot_Code__c': np.nan, 'Serial_Numbers__c': np.nan, 'Severity__c': np.nan, 'Shipping_Account_Name__c': 'Coresite', 'Shipping_City1__c': 'Somerville', 'Shipping_City__c': 'Sommerville', 'Shipping_Contact_Phone__c': 1, 'Shipping_Contact__c': 'A', 'Shipping_Country1__c': 'US', 'Shipping_Country__c': 'US', 'Shipping_Customer__c': np.nan, 'Shipping_Method__c': np.nan, 'Shipping_State1__c': 'MA', 'Shipping_State__c': 'MA', 'Shipping_Street1__c': '70 Innerbelt Road', 'Shipping_Street__c': '70 Innerbelt Rd.', 'Shipping_Zip_Code1__c': '2143', 'Shipping_Zip_Code_Postal_Code__c': '2143', 'State__c': np.nan, 'Sub_Type__c': np.nan, 'Territory__c': 'US East', 'Title__c': np.nan, 'Type__c': np.nan, 'Approved_By__c': np.nan, 'Product_Notes__c': np.nan, 'Sales_Order_Number__c': np.nan, 'What__c': np.nan, 'When__c': np.nan, 'Where__c': np.nan, 'Zipcode__c': np.nan, 'Legacy_Id__c': np.nan, 'Legacy_Case_Number__c': np.nan, 'Out_of_Box_Failure__c': False}]
        raw_data = [{
            "Id": "50046000005kA1OAAU",
            "IsDeleted": False,
            "MasterRecordId": np.nan,
            "CaseNumber": 18222,
            "ContactId": "0034600000ee0KTAAY",
            "AccountId": "0014600000ihZyBAAU",
            "Type": "Upgrade",
            "RecordTypeId": "01246000000H2ESAA0",
            "Status": "Closed",
            "Reason": np.nan,
            "Language": np.nan,
            "Subject": np.nan,
            "Priority": "Medium",
            "Description": np.nan,
            "IsClosed": True,
            "ClosedDate": "3/27/2018 17:28",
            "OwnerId": "00546000000toGQAAY",
            "CreatedDate": "1/15/2018 20:57",
            "CreatedById": "00546000000toGQAAY",
            "LastModifiedDate": "3/27/2018 17:28",
            "LastModifiedById": "00546000000toGnAAI",
            "SystemModstamp": "5/17/2018 10:30",
            "ContactPhone": 545,
            "ContactMobile": 545,
            "ContactEmail": "Blank",
            "ContactFax": np.nan,
            "Comments": np.nan,
            "LastViewedDate": np.nan,
            "LastReferencedDate": np.nan,
            "Account_Name__c": np.nan,
            "Address__c": ", , , -",
            "Billing_Account_Name__c": np.nan,
            "Billing_City__c": np.nan,
            "Billing_Contact__c": "Blank",
            "Billing_Country__c": np.nan,
            "Billing_Customer__c": np.nan,
            "Billing_State__c": np.nan,
            "Billing_Street__c": np.nan,
            "Billing_Zip_Code_Postal_Code__c": np.nan,
            "Business_Unit_Mfg_Site__c": "SPDI",
            "Business_Unit__c": "SPDI",
            "CAR_Action__c": np.nan,
            "CAR_Aging__c": np.nan,
            "CAR_Status__c": np.nan,
            "City__c": np.nan,
            "Closed_Date__c": np.nan,
            "Company_Name__c": np.nan,
            "Company__c": np.nan,
            "Completion_Status__c": np.nan,
            "Contact_Email1__c": "Blank",
            "Contact_Email__c": "Blank",
            "Contact_Name1__c": "Blank",
            "Contact_Name_lookup__c": np.nan,
            "Contact_Phone1__c": 545,
            "Contact_Phone__c": 545,
            "Contact_Title__c": np.nan,
            "Containment_Date__c": np.nan,
            "Contract_Sales_Order__c": np.nan,
            "Contract__c": np.nan,
            "Corrective_Action_Close_Date__c": np.nan,
            "Corrective_Action_Comments__c": np.nan,
            "Corrective_Action_Number__c": np.nan,
            "Corrective_Action_Start_Date__c": np.nan,
            "Corrective_Action__c": np.nan,
            "Corrective_Date__c": np.nan,
            "Cost_of_Poor_Quality_CoPQ__c": np.nan,
            "Country__c": np.nan,
            "Customer_Concern_Metric__c": np.nan,
            "Customer_Issue_Summary__c": "heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD\u221a\u00d6-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers\u221a\u00d6 NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane\"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616",
            "Customer_Issue__c": "Installation Request",
            "Date_CAR_Status_changed_from_In_CAR__c": np.nan,
            "Date_CAR_Status_changed_from_In_Process__c": np.nan,
            "Date_CAR_Status_changed_from_Open__c": np.nan,
            "Date_CAR_Status_set_to_In_CAR__c": np.nan,
            "Date_CAR_Status_set_to_In_Process__c": np.nan,
            "Date_CAR_Status_set_to_Open__c": np.nan,
            "Date_of_Response__c": np.nan,
            "Days_In_CAR__c": np.nan,
            "Days_In_Process__c": np.nan,
            "Days_in_open__c": np.nan,
            "Days_to_Response__c": np.nan,
            "Email__c": np.nan,
            "FirstName__c": "Blank",
            "Frequency__c": np.nan,
            "Identified_Department_Owner__c": np.nan,
            "Identified_Department__c": np.nan,
            "In_Process_Projected_Close_Date__c": np.nan,
            "In_Process_Start_Date__c": np.nan,
            "Internal_Defect_Code__c": np.nan,
            "Investigation_Summary__c": np.nan,
            "M2M_RMA__c": np.nan,
            "Next_Update__c": np.nan,
            "Notify_Service_SPDI__c": False,
            "Opportunity__c": np.nan,
            "Original_Sales_Order_del__c": np.nan,
            "PDI_Cost__c": np.nan,
            "Phone__c": np.nan,
            "Preventative_Date__c": np.nan,
            "Product_1__c": "110-2334",
            "Product_2__c": np.nan,
            "Product_3__c": np.nan,
            "Product_Safety__c": np.nan,
            "Product_Type_SPDI__c": np.nan,
            "Production_Supervisor_SPWR__c": np.nan,
            "Productivity_SPWR__c": np.nan,
            "Progress_Update__c": np.nan,
            "Qty_1__c": 10.0,
            "Qty_2__c": np.nan,
            "Qty_3__c": np.nan,
            "Quality_Engineer_SPWR__c": np.nan,
            "Quality_Incident_Report__c": False,
            "Quality_Technician_SPWR__c": np.nan,
            "Resolution_Summary__c": "install of retrofit has been completed",
            "Resolution__c": "Site Visit",
            "Resolved_Date__c": "Sunday, March 25, 2018",
            "PDI_Product_Family__c": "PDU",
            "Sales_Person__c": "00546000000toGIAAY",
            "Serial_Date_Lot_Code__c": np.nan,
            "Serial_Numbers__c": np.nan,
            "Severity__c": np.nan,
            "Shipping_Account_Name__c": np.nan,
            "Shipping_City1__c": "Chicago",
            "Shipping_City__c": np.nan,
            "Shipping_Contact_Phone__c": 545,
            "Shipping_Contact__c": "A",
            "Shipping_Country1__c": "US",
            "Shipping_Country__c": np.nan,
            "Shipping_Customer__c": np.nan,
            "Shipping_Method__c": np.nan,
            "Shipping_State1__c": "IL",
            "Shipping_State__c": np.nan,
            "Shipping_Street1__c": "350 East Cermak Suite 145",
            "Shipping_Street__c": np.nan,
            "Shipping_Zip_Code1__c": "60616",
            "Shipping_Zip_Code_Postal_Code__c": np.nan,
            "State__c": np.nan,
            "Sub_Type__c": np.nan,
            "Territory__c": "US Central",
            "Title__c": np.nan,
            "Type__c": np.nan,
            "Approved_By__c": np.nan,
            "Product_Notes__c": np.nan,
            "Sales_Order_Number__c": np.nan,
            "What__c": np.nan,
            "When__c": np.nan,
            "Where__c": np.nan,
            "Zipcode__c": np.nan,
            "Legacy_Id__c": np.nan,
            "Legacy_Case_Number__c": np.nan,
            "Out_of_Box_Failure__c": False
      }]

        df_services_raw = pd.DataFrame(raw_data)

        serial_num =  [{'SerialNumber': '110-2334', 'Id': '50046000003p5gwAAA', 'SerialNumberContract': '110-2334', 'Qty': 4, 'src': 'Product_1__c'}]
        serial_num = [{
            "SerialNumber": "110-2334",
            "Id": "50046000004NQ4sAAG",
            "SerialNumberContract": "survey 10 PDUs (110-2334) that Digital would like to add a side car upgrade. Look for space around the unit, inside the units, etc \r\n\r\nsite \r\nDigital Realty \r\nsuite S820 \r\n350 E. Cermak \r\nChicago, IL",
            "Qty": 0.0,
            "src": "Customer_Issue_Summary__c"
      }]

        df_services_serialnum = pd.DataFrame(serial_num)
        expected_op = pd.DataFrame([{'Id': ['50046000005kA1OAAU', '50046000005kA1OAAU', '50046000005kA1OAAU', '50046000005kA1OAAU', '50046000005kA1OAAU', '50046000005kA1OAAU', '50046000005kA1OAAU', '50046000005kA1OAAU', '50046000005kA1OAAU', '50046000005kA1OAAU', '50046000003elIkAAI', '50046000003elIkAAI', '50046000003elIkAAI', '50046000003elIkAAI', '50046000003elIkAAI', '50046000003elIkAAI', '50046000003elIkAAI', '50046000003elIkAAI', '50046000000rCI4AAM', '50046000000rCI4AAM', '50046000000rCI4AAM', '50046000000rCI4AAM', '50046000000rCHfAAM', '50046000000rCHfAAM', '50046000000rCHfAAM', '50046000000rCHfAAM', '50046000000rCHfAAM', '50046000000rCHfAAM', '50046000001FeY3AAK', '50046000001FeY3AAK'], 'Customer_Issue_Summary__c': ['heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'heat mitigation retrofit kit\r\n10 PDUs  110-2334\r\n\r\nEach kit includes:\r\nFIELD RETROFIT KIT NO SIDECAR\r\nEach Kit Includes:\r\nHeat Mitigation Kit for standard DLR unit\r\n 100% Rated Electronic Trip SQD\r\nNeta Tested Breakers Mounted to Brackets at Factory\r\nIncludes:\r\n- Lexan Barriers\r\n-- Vented Assemblies  \r\n-Access Panel\r\n- 2/0 Phase Wire To Be Pre-cut At Factory\r\nQTY (6) Each per Unit\r\nModel Number: SQD JGL36250CU31X\r\nPN: CKB15430T\r\nDescription:\r\nSQD√Ö-JGL,3P,250AF,600V, LI, 100% rated\r\nSet Breaker To Trip at 225A\r\nElectronic Trip Breakers√Ö NETA Tested\r\nPDU Ventilation Kit  Includes:\r\n(1) PNL18699 Left Hand Side PDU Vented Panel\r\n(1) PNL18700 Rear PDU Vented Pane"   \r\n\r\nsite\r\nDigital Realty\r\nsuite 820\r\n350 E Cermak\r\nChicago, IL  60616', 'Sidecar upgrade for heat mitigation kit on 8 PDUs\r\n\r\nsite\r\nDigital Realty\r\n1232 Alma Rd\r\nsuite 150\r\nRichardson, TX  75081', 'Sidecar upgrade for heat mitigation kit on 8 PDUs\r\n\r\nsite\r\nDigital Realty\r\n1232 Alma Rd\r\nsuite 150\r\nRichardson, TX  75081', 'Sidecar upgrade for heat mitigation kit on 8 PDUs\r\n\r\nsite\r\nDigital Realty\r\n1232 Alma Rd\r\nsuite 150\r\nRichardson, TX  75081', 'Sidecar upgrade for heat mitigation kit on 8 PDUs\r\n\r\nsite\r\nDigital Realty\r\n1232 Alma Rd\r\nsuite 150\r\nRichardson, TX  75081', 'Sidecar upgrade for heat mitigation kit on 8 PDUs\r\n\r\nsite\r\nDigital Realty\r\n1232 Alma Rd\r\nsuite 150\r\nRichardson, TX  75081', 'Sidecar upgrade for heat mitigation kit on 8 PDUs\r\n\r\nsite\r\nDigital Realty\r\n1232 Alma Rd\r\nsuite 150\r\nRichardson, TX  75081', 'Sidecar upgrade for heat mitigation kit on 8 PDUs\r\n\r\nsite\r\nDigital Realty\r\n1232 Alma Rd\r\nsuite 150\r\nRichardson, TX  75081', 'Sidecar upgrade for heat mitigation kit on 8 PDUs\r\n\r\nsite\r\nDigital Realty\r\n1232 Alma Rd\r\nsuite 150\r\nRichardson, TX  75081', 'To remove (4) sidecars on PDUs and move to pallets.\r\n110-4429\r\n\r\nSite address:\r\nStream\r\n1708 West Creek Lane\r\nChaska, MN 55318\r\n\r\nContact:\r\nJohn Oliver\r\n(612) 490-8336', 'To remove (4) sidecars on PDUs and move to pallets.\r\n110-4429\r\n\r\nSite address:\r\nStream\r\n1708 West Creek Lane\r\nChaska, MN 55318\r\n\r\nContact:\r\nJohn Oliver\r\n(612) 490-8336', 'To remove (4) sidecars on PDUs and move to pallets.\r\n110-4429\r\n\r\nSite address:\r\nStream\r\n1708 West Creek Lane\r\nChaska, MN 55318\r\n\r\nContact:\r\nJohn Oliver\r\n(612) 490-8336', 'To remove (4) sidecars on PDUs and move to pallets.\r\n110-4429\r\n\r\nSite address:\r\nStream\r\n1708 West Creek Lane\r\nChaska, MN 55318\r\n\r\nContact:\r\nJohn Oliver\r\n(612) 490-8336', '(6) retrofit side car upgrade                \r\nserial # 350-0269\r\n\r\nRetrofit 9¬Æ Deep Side-facing Right-Hand Sidecars for existing PDI PDUs onsite (SN 350-0269):\r\nEach Sidecar Includes:\r\n Qty (2) 400 Amp, Square-D, 3-Pole Output Subfeed Circuit Breakers, 400AF / 400AT, 65kAIC @ 240V,\r\n80% rated for continuous load. Top cable exit.\r\n\r\nSite address:\r\nVerizon Wireless\r\n7400 World Communications Drive\r\nOmaha, NE\r\n\r\nContact:\r\nMark Hayward\r\n402-943-6912', '(6) retrofit side car upgrade                \r\nserial # 350-0269\r\n\r\nRetrofit 9¬Æ Deep Side-facing Right-Hand Sidecars for existing PDI PDUs onsite (SN 350-0269):\r\nEach Sidecar Includes:\r\n Qty (2) 400 Amp, Square-D, 3-Pole Output Subfeed Circuit Breakers, 400AF / 400AT, 65kAIC @ 240V,\r\n80% rated for continuous load. Top cable exit.\r\n\r\nSite address:\r\nVerizon Wireless\r\n7400 World Communications Drive\r\nOmaha, NE\r\n\r\nContact:\r\nMark Hayward\r\n402-943-6912', '(6) retrofit side car upgrade                \r\nserial # 350-0269\r\n\r\nRetrofit 9¬Æ Deep Side-facing Right-Hand Sidecars for existing PDI PDUs onsite (SN 350-0269):\r\nEach Sidecar Includes:\r\n Qty (2) 400 Amp, Square-D, 3-Pole Output Subfeed Circuit Breakers, 400AF / 400AT, 65kAIC @ 240V,\r\n80% rated for continuous load. Top cable exit.\r\n\r\nSite address:\r\nVerizon Wireless\r\n7400 World Communications Drive\r\nOmaha, NE\r\n\r\nContact:\r\nMark Hayward\r\n402-943-6912', '(6) retrofit side car upgrade                \r\nserial # 350-0269\r\n\r\nRetrofit 9¬Æ Deep Side-facing Right-Hand Sidecars for existing PDI PDUs onsite (SN 350-0269):\r\nEach Sidecar Includes:\r\n Qty (2) 400 Amp, Square-D, 3-Pole Output Subfeed Circuit Breakers, 400AF / 400AT, 65kAIC @ 240V,\r\n80% rated for continuous load. Top cable exit.\r\n\r\nSite address:\r\nVerizon Wireless\r\n7400 World Communications Drive\r\nOmaha, NE\r\n\r\nContact:\r\nMark Hayward\r\n402-943-6912', '(6) retrofit side car upgrade                \r\nserial # 350-0269\r\n\r\nRetrofit 9¬Æ Deep Side-facing Right-Hand Sidecars for existing PDI PDUs onsite (SN 350-0269):\r\nEach Sidecar Includes:\r\n Qty (2) 400 Amp, Square-D, 3-Pole Output Subfeed Circuit Breakers, 400AF / 400AT, 65kAIC @ 240V,\r\n80% rated for continuous load. Top cable exit.\r\n\r\nSite address:\r\nVerizon Wireless\r\n7400 World Communications Drive\r\nOmaha, NE\r\n\r\nContact:\r\nMark Hayward\r\n402-943-6912', '(6) retrofit side car upgrade                \r\nserial # 350-0269\r\n\r\nRetrofit 9¬Æ Deep Side-facing Right-Hand Sidecars for existing PDI PDUs onsite (SN 350-0269):\r\nEach Sidecar Includes:\r\n Qty (2) 400 Amp, Square-D, 3-Pole Output Subfeed Circuit Breakers, 400AF / 400AT, 65kAIC @ 240V,\r\n80% rated for continuous load. Top cable exit.\r\n\r\nSite address:\r\nVerizon Wireless\r\n7400 World Communications Drive\r\nOmaha, NE\r\n\r\nContact:\r\nMark Hayward\r\n402-943-6912', 'I know Thun just came out to the site for repairs, however two of the ones that he was onsite for went back into alarm. Please see the below list. As well could I get a follow up from when Eli was onsite performing his last repairs. There was a jcomm board that was giving erroneous readings and Eli was going to discuss what we could do to get the correct readings from the board/RPP. \r\n\r\nA1E1: TX down  (442-0048-13B)\r\nC2D2: TX Down  (442-0048-21A)\r\nE2D2: TX Down  (442-0060-2B)', 'I know Thun just came out to the site for repairs, however two of the ones that he was onsite for went back into alarm. Please see the below list. As well could I get a follow up from when Eli was onsite performing his last repairs. There was a jcomm board that was giving erroneous readings and Eli was going to discuss what we could do to get the correct readings from the board/RPP. \r\n\r\nA1E1: TX down  (442-0048-13B)\r\nC2D2: TX Down  (442-0048-21A)\r\nE2D2: TX Down  (442-0060-2B)'], 'SerialNumberOrg': ['110-2334', '110-2334', '110-2334', '110-2334', '110-2334', '110-2334', '110-2334', '110-2334', '110-2334', '110-2334', '110-2710', '110-2710', '110-2710', '110-2711', '110-2711', '110-2711', '110-2711', '110-2711', '110-4429', '110-4429', '110-4429', '110-4429', '350-0269', '350-0269', '350-0269', '350-0269', '350-0269', '350-0269', '442-0048', '442-0048'], 'Has_JCOMM': [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, True, True], 'Qty': [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 3.0, 3.0, 3.0, 5.0, 5.0, 5.0, 5.0, 5.0, 4.0, 4.0, 4.0, 4.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 2.0, 2.0], 'Has_Sidecar': [True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, np.nan, np.nan], 'SerialNumber': ['110-2334-1', '110-2334-10', '110-2334-2', '110-2334-3', '110-2334-4', '110-2334-5', '110-2334-6', '110-2334-7', '110-2334-8', '110-2334-9', '110-2710-1', '110-2710-2', '110-2710-3', '110-2711-1', '110-2711-2', '110-2711-3', '110-2711-4', '110-2711-5', '110-4429-1', '110-4429-2', '110-4429-3', '110-4429-4', '350-0269-1', '350-0269-2', '350-0269-3', '350-0269-4', '350-0269-5', '350-0269-6', '442-0048-1', '442-0048-2'], 'KeySerial': ['contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract', 'contract']}])
        with pytest.raises(Exception) as info:
            obj_services.pipline_component_identify(df_services_raw, df_services_serialnum)
            assert info == Exception

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
