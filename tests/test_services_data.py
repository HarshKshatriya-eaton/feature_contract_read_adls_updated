"""@file test_services_data.py.

@brief This file used to test code for services data.




@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# Pytest execution command
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
class TestServicesMainFunc:

    def test_pipline_main(self):
        """
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

    def test_pipline_init(self):
        """
        """
        with pytest.raises(Exception) as info:
            obj_services.__init__(self)
            assert info.type == Exception

    def test_pipline_jcomm(self):
        """
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

    @pytest.mark.parametrize(
        "hardware_changes",
        [
            '5004600000', 'A2GHqAAN	"customer received the display and installed it.  However, display '
                          'was not working properly.We had a technician troubleshoot over the phone.  '
                          'it was decided to send a replacement display site Upsistemas Brazil	',
            'PCBA - Hardware Issue,	"replacement board shipped 6/26 UPS 1z2698840352736805',
            'Hardware/Parts Provided', 'Display', 'replace'
        ],
    )
    def test_services_merge_data(self,hardware_changes):
        """
        """
        # Map the column value to the data value.
        hardwareDf = pd.DataFrame(data = {''})
        hardware_changes_ = hardware_changes
        df_services_raw = hardware_changes_
        df_sr_num = hardware_changes_
        expected_output = hardware_changes_
        actual_output = obj_services.merge_data(hardware_changes,df_services_raw,df_sr_num)
        # with pytest.raises(Exception) as info:
            # obj_services.merge_data(self,hardware_changes,df_services_raw,df_sr_num)
        assert all([a == b for a, b in zip(actual_output, expected_output)])


# Testcases for extracting raw serial number data
# pipeline_serial_number
    def test_pipeline_serial_number(self):
        """
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


# %%
if __name__ == "__main__":
    testclass = TestServicesMainFunc()
    testclass.test_main_func()
