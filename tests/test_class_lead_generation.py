"""@file test_class_lead_generation_data.py.

@brief This file used to test code for Lead Generation Class




@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
from datetime import datetime
from datetime import timedelta
import random

import numpy as np
import pytest
import pandas as pd
from pandas import Timestamp, NaT
from pandas._testing import assert_frame_equal
from utils.dcpd.class_lead_generation import LeadGeneration

obj_lead = LeadGeneration()


class TestPipelineLead:
    """
    Check if pipeline merge method in lead generation are merged correctly.
    """

    def test_pipeline_add_jcomm_sidecar(self):
        data_leads = {
            'SerialNumber_M2M': [101, 102, 103, 104, 105],
            'SerialNumber': [1, 2, 3, 4, 5]
        }

        data_service_jcomm_sidecar = {
            'SerialNumber': [101, 102, 103, 104, 105],
            'Has_JCOMM': [True, False, True, False, True],
            'Has_Sidecar': [False, True, True, False, True]
        }

        expected_data = {
            'Has_JCOMM': [True, False, True, False, True],
            'Has_Sidecar': [False, True, True, False, True],
            'SerialNumber': [1, 2, 3, 4, 5],
            'SerialNumber_M2M': [101, 102, 103, 104, 105]
        }

        df_leads = pd.DataFrame(data_leads)

        df_service_jcomm_sidecar = pd.DataFrame(data_service_jcomm_sidecar)

        expected_df = pd.DataFrame(expected_data)

        result_df = obj_lead.pipeline_add_jcomm_sidecar(df_leads,
                                                        service_df=df_service_jcomm_sidecar)

        assert_frame_equal(result_df.sort_index(axis=1), expected_df.sort_index(axis=1),
                           check_dtype=False, check_exact=False, check_names=True)

    def test_classify_lead(self):
        today_date = datetime.now().date()

        # Generate date_code values
        date_code_values = [(today_date - timedelta(days=random.randint(0, 365))) for _ in range(2)]
        date_code_values += [(today_date - timedelta(days=random.randint(365, 730))) for _ in
                             range(4)]
        random.shuffle(date_code_values)

        data = {
            'EOSL': ['2023', '2024', '', '2026', '2025', ''],
            'Life__Years': [15.0, 10.0, None, 5.0, None, 8.0],
            'date_code': date_code_values
        }

        expected_data = {
            'lead_type': ['EOSL', 'EOSL', 'EOSL', 'EOSL', 'Life', 'Life', 'Life', 'Life'],
            'flag_include': [np.nan, np.nan, np.nan, np.nan, False, False, False, False]
        }

        df_leads_wn_class = pd.DataFrame(data)

        expected_df = pd.DataFrame(expected_data)

        result_df = obj_lead.classify_lead(df_leads_wn_class, test_services=pd.DataFrame())

        result_df = result_df[['lead_type', 'flag_include']]

        result_df.reset_index(drop=True, inplace=True)
        expected_df.reset_index(drop=True, inplace=True)

        assert_frame_equal(result_df, expected_df,
                           check_dtype=False, check_exact=False, check_names=True)

        with pytest.raises(Exception) as info:
            obj_lead.classify_lead(data, test_services=pd.DataFrame())
            assert info.type == Exception

    def test_id_leads_for_partno(self):
        install_data = {
            'Job_Index': [1, 2, 3, 4, 5],
            'PartNumber_TLN_BOM': ['TLN001', 'TLN002', 'TLN003', 'TLN004', 'TLN005'],
            'PartNumber_BOM_BOM': ['BOM001', 'BOM002', 'BOM003', 'BOM004', 'BOM005'],
            'Total_Quantity': [10, 5, 8, 12, 6],
            'InstallDate': ['2022-01-01', '2022-02-15', '2022-03-10', '2022-04-20', '2022-05-05'],
            'Product_M2M': ['pdu', 'rpp', 'sts', 'pdu', 'rpp'],
            'SerialNumber_M2M': ['SN001', 'SN002', 'SN003', 'SN004', 'SN005'],
            'key_value': ['pdu', 'rpp', 'sts', 'pdu', 'rpp']
        }

        ref_data = {
            'Product_M2M': ['PDU', 'RPP', 'STS', 'PDU', 'RPP'],
            'Match': ['exact', 'contains', 'begins_with', 'contains', 'exact'],
            'Component': [None, 'FAN', 'BREAKER', None, 'DISPLAY'],
            'Component_Description': ['Description 1', 'Description 2', 'Description 3',
                                      'Description 4', 'Description 5'],
            'End of Prod': ['2023-01-01', '2024-02-15', '2025-03-10', '2026-04-20', '2027-05-05'],
            'Status': ['Active', 'Active', 'Active', 'Inactive', 'Active'],
            'Life__Years': [10, 15, 12, 8, 20],
            'EOSL': [2033, 2039, 2037, 2035, 2042],
            'flag_raise_in_gp': [True, False, True, False, True]
        }

        expected_data = {'Job_Index': [2, 5, 1, 4, 3], 'Total_Quantity': [5, 6, 10, 12, 8],
                         'Component': ['DISPLAY', 'DISPLAY', None, None, 'BREAKER'],
                         'Component_Description': ['Description 5', 'Description 5',
                                                   'Description 4', 'Description 4',
                                                   'Description 3'],
                         'key': ['rpp', 'rpp', 'pdu', 'pdu', 'sts'],
                         'End of Prod': ['2027-05-05', '2027-05-05', '2026-04-20', '2026-04-20',
                                         '2025-03-10'],
                         'Status': ['Active', 'Active', 'Inactive', 'Inactive', 'Active'],
                         'Life__Years': [20.0, 20.0, 8.0, 8.0, 12.0],
                         'EOSL': [2042.0, 2042.0, 2035.0, 2035.0, 2037.0],
                         'flag_raise_in_gp': [True, True, False, False, True],
                         'SerialNumber_M2M': ['SN002', 'SN005', 'SN001', 'SN004', 'SN003'],
                         'InstallDate': ['2022-02-15', '2022-05-05', '2022-01-01', '2022-04-20',
                                         '2022-03-10'],
                         'key_value': ['rpp', 'rpp', 'pdu', 'pdu', 'sts'],
                         'lead_match': ['exact', 'exact', 'contains', 'contains', 'begins_with'],
                         'lead_id_basedon': ['Product_M2M', 'Product_M2M', 'Product_M2M',
                                             'Product_M2M', 'Product_M2M']}

        df_data = pd.DataFrame(install_data)
        ref_lead = pd.DataFrame(ref_data)
        expected_df = pd.DataFrame(expected_data)

        result_df = obj_lead.id_leads_for_partno(df_data, ref_lead, 'Product_M2M')

        result_df.reset_index(drop=True, inplace=True)
        expected_df.reset_index(drop=True, inplace=True)

        assert_frame_equal(result_df.sort_index(axis=1), expected_df.sort_index(axis=1),
                           check_dtype=False, check_exact=False, check_names=True)

        with pytest.raises(Exception) as info:
            obj_lead.id_leads_for_partno(install_data, install_data, 'Product_M2M')
            assert info.type == Exception

    def test_pipeline_merge_lead_services(self):
        # Sample data for df_services
        data_services = {
            'SerialNumber': ['SN001', 'SN002', 'SN003', 'SN004', 'SN005'],
            'component': ['BCMS', 'FAN', 'BREAKER', 'DISPLAY', 'PCB'],
            'ClosedDate': ['2022-01-01', '2023-02-15', '', '2022-04-20', '']
        }

        df_services = pd.DataFrame(data_services)

        # Sample data for df_leads
        data_leads = {
            'SerialNumber_M2M': ['SN001', 'SN002', 'SN006', 'SN007', 'SN008'],
            'InstallDate': ['2022-01-01', '2023-02-15', '2022-03-10', '2022-04-20',
                            '2022-05-05'],
            'Component': ['BCMS', 'FAN', 'BREAKER', 'DISPLAY', 'PCB']
        }

        expected_data = {
            'SerialNumber_M2M': ['SN001', 'SN002', 'SN006', 'SN007', 'SN008'],
            'InstallDate': ['2022-01-01', '2023-02-15', '2022-03-10', '2022-04-20', '2022-05-05'],
            'Component': ['BCMS', 'FAN', 'BREAKER', 'DISPLAY', 'PCB'],
            'temp_column': ['bcms', 'fan', 'breaker', 'display', 'pcb'],
            'SerialNumber': ['SN001', 'SN002', '', '', ''],
            'component': ['bcms', 'fan', '', '', ''],
            'ClosedDate': ['2022-01-01', '2023-02-15', '', '', ''],
            'date_code': ['2022-01-01', '2023-02-15', '2022-03-10', '2022-04-20', '2022-05-05']}

        df_leads = pd.DataFrame(data_leads)

        result_df = obj_lead.pipeline_merge_lead_services(df_leads, df_services)

        expected_df = pd.DataFrame(expected_data)

        result_df.reset_index(drop=True, inplace=True)
        expected_df.reset_index(drop=True, inplace=True)

        assert_frame_equal(result_df.sort_index(axis=1), expected_df.sort_index(axis=1),
                           check_dtype=False, check_exact=False, check_names=True)

        with pytest.raises(Exception) as info:
            obj_lead.pipeline_merge_lead_services(data_leads, data_leads)
            assert info.type == Exception

