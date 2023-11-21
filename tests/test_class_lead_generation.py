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
obj_lead.config['file']['dir_data'] = "./tests/ip"
obj_lead.config['file']['dir_ref'] = "./tests/ip"
obj_lead.config['file']['dir_results'] = "./tests/"
obj_lead.config['file']['dir_validation'] = "ip"
obj_lead.config['file']['dir_intermediate'] = "ip"

# class TestPipelineLead:
#     """
#     Check if pipeline merge method in lead generation are merged correctly.
#     """
#
#     def test_pipeline_add_jcomm_sidecar(self):
#         data_leads = {
#             'SerialNumber_M2M': [101, 102, 103, 104, 105],
#             'SerialNumber': [1, 2, 3, 4, 5]
#         }
#
#         data_service_jcomm_sidecar = {
#             'SerialNumber': [101, 102, 103, 104],
#             'Has_JCOMM': [True, False, True, False],
#             'Has_Sidecar': [False, True, True, False]
#         }
#
#         expected_data = {
#             'Has_JCOMM': [True, False, True, False, np.nan],
#             'Has_Sidecar': [False, True, True, False, np.nan],
#             'SerialNumber': [1, 2, 3, 4, 5],
#             'SerialNumber_M2M': [101, 102, 103, 104, 105]
#         }
#
#         df_leads = pd.DataFrame(data_leads)
#
#         df_service_jcomm_sidecar = pd.DataFrame(data_service_jcomm_sidecar)
#
#         expected_df = pd.DataFrame(expected_data)
#
#         result_df = obj_lead.pipeline_add_jcomm_sidecar(df_leads,
#                                                         service_df=df_service_jcomm_sidecar)
#
#         assert_frame_equal(result_df.sort_index(axis=1), expected_df.sort_index(axis=1),
#                            check_dtype=False, check_exact=False, check_names=True)
#
#     def test_classify_lead(self):
#         today_date = datetime.now().date()
#
#         # Generate date_code values
#         date_code_values = [(today_date - timedelta(days=random.randint(0, 365))) for _ in range(2)]
#         date_code_values += [(today_date - timedelta(days=random.randint(365, 730))) for _ in
#                              range(4)]
#         random.shuffle(date_code_values)
#
#         data = {
#             'EOSL': ['2023', '2024', '', '2026', '2025', ''],
#             'Life__Years': [15.0, 10.0, None, 5.0, None, 1.0],
#             'date_code': date_code_values
#         }
#
#         expected_data = {
#             'lead_type': ['EOSL', 'EOSL', 'EOSL', 'EOSL', 'Life'],
#             'flag_include': [np.nan, np.nan, np.nan, np.nan, False]
#         }
#
#         df_leads_wn_class = pd.DataFrame(data)
#
#         expected_df = pd.DataFrame(expected_data)
#
#         result_df = obj_lead.classify_lead(df_leads_wn_class, test_services=pd.DataFrame())
#
#         result_df = result_df[['lead_type', 'flag_include']]
#
#         result_df.reset_index(drop=True, inplace=True)
#         expected_df.reset_index(drop=True, inplace=True)
#
#         assert_frame_equal(result_df, expected_df,
#                            check_dtype=False, check_exact=False, check_names=True)
#
#         with pytest.raises(Exception) as info:
#             obj_lead.classify_lead(data, test_services=pd.DataFrame())
#             assert info.type == Exception
#
#     def test_id_leads_for_partno_product_m2m(self):
#         install_data = {
#             'Job_Index': [1, 2, 3, 4, 5],
#             'Total_Quantity': [10, 5, 8, 12, 6],
#             'InstallDate': ['2022-01-01', '2022-02-15', '2022-03-10', '2022-04-20', '2022-05-05'],
#             'Product_M2M': ['pdu', 'rpp', 'sts', 'pdu', 'rpp'],
#             'PartNumber_BOM_BOM': ['FAN', 'PREFANPOST', 'FAN001', 'Color Display', 'Display'],
#             'SerialNumber_M2M': ['SN001', 'SN002', 'SN003', 'SN004', 'SN005'],
#             'key_value': ['pdu', 'rpp', 'sts', 'pdu', 'rpp']
#         }
#
#         ref_data = {
#             'Product_M2M': ['PDU', 'RPP', 'STS'],
#             'Match': ['contains', 'exact', 'contains'],
#             'Component': ['PDU', 'RPP', 'STS'],
#             'Component_Description': ['Description 1', 'Description 2', 'Description 3'],
#             'End of Prod': ['2023-01-01', '2024-02-15', '2025-03-10'],
#             'Status': ['Active', 'Active', 'Active'],
#             'Life__Years': [10, 15, 12],
#             'EOSL': [2033, 2039, 2037],
#             'flag_raise_in_gp': [True, False, True]
#         }
#
#         expected_data = {
#             'Job_Index': [2, 5, 1, 4, 3],
#             'Total_Quantity': [5, 6, 10, 12, 8],
#             'Component': ['RPP', 'RPP', 'PDU', 'PDU', 'STS'],
#             'Component_Description': ['Description 2', 'Description 2', 'Description 1',
#                                       'Description 1', 'Description 3'],
#             'key': ['rpp', 'rpp', 'pdu', 'pdu', 'sts'],
#             'End of Prod': ['2024-02-15', '2024-02-15', '2023-01-01', '2023-01-01', '2025-03-10'],
#             'Status': ['Active', 'Active', 'Active', 'Active', 'Active'],
#             'Life__Years': [15.0, 15.0, 10.0, 10.0, 12.0],
#             'EOSL': [2039.0, 2039.0, 2033.0, 2033.0, 2037.0],
#             'flag_raise_in_gp': [False, False, True, True, True],
#             'SerialNumber_M2M': ['SN002', 'SN005', 'SN001', 'SN004', 'SN003'],
#             'InstallDate': ['2022-02-15', '2022-05-05', '2022-01-01', '2022-04-20', '2022-03-10'],
#             'key_value': ['rpp', 'rpp', 'pdu', 'pdu', 'sts'],
#             'lead_match': ['exact', 'exact', 'contains', 'contains', 'contains'],
#             'lead_id_basedon': ['Product_M2M', 'Product_M2M', 'Product_M2M', 'Product_M2M',
#                                 'Product_M2M']}
#
#         df_data = pd.DataFrame(install_data)
#         ref_lead = pd.DataFrame(ref_data)
#         expected_df = pd.DataFrame(expected_data)
#
#         result_df = obj_lead.id_leads_for_partno(df_data, ref_lead, 'Product_M2M')
#
#         result_df.reset_index(drop=True, inplace=True)
#         expected_df.reset_index(drop=True, inplace=True)
#
#         assert_frame_equal(result_df.sort_index(axis=1), expected_df.sort_index(axis=1),
#                            check_dtype=False, check_exact=False, check_names=True)
#
#         with pytest.raises(Exception) as info:
#             obj_lead.id_leads_for_partno(install_data, install_data, 'Product_M2M')
#             assert info.type == Exception
#
#     def test_id_leads_for_partno_bom_bom(self):
#         install_data = {
#             'Job_Index': [1, 2, 3, 4, 5],
#             'Total_Quantity': [10, 5, 8, 12, 6],
#             'InstallDate': ['2022-01-01', '2022-02-15', '2022-03-10', '2022-04-20', '2022-05-05'],
#             'Product_M2M': ['pdu', 'rpp', 'sts', 'pdu', 'rpp'],
#             'PartNumber_BOM_BOM': ['FAN1', 'FAN01', 'FAN0101', 'FAN2', 'FAN1'],
#             'SerialNumber_M2M': ['SN001', 'SN002', 'SN003', 'SN004', 'SN005'],
#             'key_value': ['pdu', 'rpp', 'sts', 'pdu', 'rpp']
#         }
#
#         ref_data = {
#             'PartNumber_BOM_BOM': ['FAN1', 'FAN01', 'FAN2', 'FAN1'],
#             'Match': ['exact', 'contains', 'begins_with', 'exact'],
#             'Component': ['BCMS', 'M4 Display', 'Breaker', 'Color Display'],
#             'Component_Description': ['Description 1', 'Description 2', 'Description 3',
#                                       'Description 4'],
#             'End of Prod': ['2023-01-01', '2024-02-15', '2025-03-10', '2025-03-12'],
#             'Status': ['Active', 'Active', 'Active', 'Active'],
#             'Life__Years': [10, 15, 12, 15],
#             'EOSL': [2033, 2039, 2037, 2034],
#             'flag_raise_in_gp': [True, False, True, True]
#         }
#
#         expected_data = {
#             'Job_Index': [1, 1, 5, 5, 2, 3, 4],
#             'Total_Quantity': [10, 10, 6, 6, 5, 8, 12],
#             'Component': ['BCMS', 'Color Display', 'BCMS', 'Color Display', 'M4 Display',
#                           'M4 Display', 'Breaker'],
#             'Component_Description': ['Description 1', 'Description 4', 'Description 1',
#                                       'Description 4', 'Description 2', 'Description 2',
#                                       'Description 3'],
#             'key': ['fan1', 'fan1', 'fan1', 'fan1', 'fan01', 'fan01', 'fan2'],
#             'End of Prod': ['2023-01-01', '2025-03-12', '2023-01-01', '2025-03-12', '2024-02-15',
#                             '2024-02-15', '2025-03-10'],
#             'Status': ['Active', 'Active', 'Active', 'Active', 'Active', 'Active', 'Active'],
#             'Life__Years': [10.0, 15.0, 10.0, 15.0, 15.0, 15.0, 12.0],
#             'EOSL': [2033.0, 2034.0, 2033.0, 2034.0, 2039.0, 2039.0, 2037.0],
#             'flag_raise_in_gp': [True, True, True, True, False, False, True],
#             'SerialNumber_M2M': ['SN001', 'SN001', 'SN005', 'SN005', 'SN002', 'SN003', 'SN004'],
#             'InstallDate': ['2022-01-01', '2022-01-01', '2022-05-05', '2022-05-05', '2022-02-15',
#                             '2022-03-10', '2022-04-20'],
#             'key_value': ['pdu', 'pdu', 'rpp', 'rpp', 'rpp', 'sts', 'pdu'],
#             'lead_match': ['exact', 'exact', 'exact', 'exact', 'contains', 'contains',
#                            'begins_with'],
#             'lead_id_basedon': ['PartNumber_BOM_BOM', 'PartNumber_BOM_BOM', 'PartNumber_BOM_BOM',
#                                 'PartNumber_BOM_BOM', 'PartNumber_BOM_BOM', 'PartNumber_BOM_BOM',
#                                 'PartNumber_BOM_BOM']}
#
#         df_data = pd.DataFrame(install_data)
#         ref_lead = pd.DataFrame(ref_data)
#         expected_df = pd.DataFrame(expected_data)
#
#         result_df = obj_lead.id_leads_for_partno(df_data, ref_lead, 'PartNumber_BOM_BOM')
#
#         result_df.reset_index(drop=True, inplace=True)
#
#         expected_df.reset_index(drop=True, inplace=True)
#
#         assert_frame_equal(result_df.sort_index(axis=1), expected_df.sort_index(axis=1),
#                            check_dtype=False, check_exact=False, check_names=True)
#
#     def test_pipeline_merge_lead_services(self):
#         # Sample data for df_services
#         data_services = {
#             'SerialNumber': ['SN001', 'SN002', 'SN003', 'SN004', 'SN005'],
#             'component': ['BCMS', 'FAN', 'BREAKER', 'DISPLAY', 'PCB'],
#             'ClosedDate': ['2022-01-01', '2023-02-15', '', '2022-04-20', '']
#         }
#
#         df_services = pd.DataFrame(data_services)
#
#         # Sample data for df_leads
#         data_leads = {
#             'SerialNumber_M2M': ['SN001', 'SN002', 'SN006', 'SN007', 'SN008'],
#             'InstallDate': ['2022-01-01', '2023-02-15', '2022-03-10', '2022-04-20',
#                             '2022-05-05'],
#             'Component': ['BCMS', 'FAN', 'BREAKER', 'DISPLAY', 'PCB']
#         }
#
#         expected_data = {
#             'SerialNumber_M2M': ['SN001', 'SN002', 'SN006', 'SN007', 'SN008'],
#             'InstallDate': ['2022-01-01', '2023-02-15', '2022-03-10', '2022-04-20', '2022-05-05'],
#             'Component': ['BCMS', 'FAN', 'BREAKER', 'DISPLAY', 'PCB'],
#             'temp_column': ['bcms', 'fan', 'breaker', 'display', 'pcb'],
#             'component': ['bcms', 'fan', '', '', ''],
#             'ClosedDate': ['2022-01-01', '2023-02-15', '', '', ''],
#             'date_code': ['2022-01-01', '2023-02-15', '2022-03-10', '2022-04-20', '2022-05-05'],
#             'source': ['Services', 'Services', 'InstallBase', 'InstallBase', 'InstallBase']}
#
#         df_leads = pd.DataFrame(data_leads)
#
#         result_df = obj_lead.pipeline_merge_lead_services(df_leads, df_services)
#
#         expected_df = pd.DataFrame(expected_data)
#
#         result_df.reset_index(drop=True, inplace=True)
#         expected_df.reset_index(drop=True, inplace=True)
#
#         assert_frame_equal(result_df.sort_index(axis=1), expected_df.sort_index(axis=1),
#                            check_dtype=False, check_exact=False, check_names=True)
#
#         with pytest.raises(Exception) as info:
#             obj_lead.pipeline_merge_lead_services(data_leads, data_leads)
#             assert info.type == Exception

class TestAddDataMTS:
    @pytest.mark.parametrize(
        "df_install_mts",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_add_data_mts_err(self, df_install_mts):
        with pytest.raises(Exception) as _:
            df_out_mts = obj_lead.add_data_mts(df_install_mts, merge_type='left')

    def test_add_data_mts_valid_scenario(self):
        df_install_mts = pd.read_csv("tests/ip/df_install_mts.csv")
        df_out_mts_ac = obj_lead.add_data_mts(df_install_mts, merge_type='left')
        df_out_mts_exp = pd.read_csv("tests/ip/df_out_mts_exp.csv")
        df_out_mts_exp = df_out_mts_exp.fillna("")
        df_out_mts_ac = df_out_mts_ac.fillna("")
        assert_frame_equal(df_out_mts_ac, df_out_mts_exp)

class TestTestContractInstall:
    def test_contract_install_valid_scenario(self):
        contract_install = obj_lead.pipeline_contract_install()
        contract_install_exp = pd.read_csv("tests/ip/contract_install_exp.csv")
        contract_install_exp = contract_install_exp.fillna("")
        contract_install = contract_install.fillna("")
        assert_frame_equal(contract_install_exp, contract_install)

class TestPostProcessLeads:
    @pytest.mark.parametrize(
        "df_leads",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_post_proecess_leads_err(self, df_leads):
        with pytest.raises(Exception) as _:
            filtered_data = obj_lead.post_proecess_leads(df_leads)

    def test_post_proecess_leads_valid_scenario(self):
        df_install = pd.read_csv("./ip/df_install_filter_mtmdata.csv")
        filtered_data = obj_lead.post_proecess_leads(df_install)

class TestPostProcessRefInstall:
    @pytest.mark.parametrize(
        "ref_install",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_post_process_ref_install_err(self, ref_install):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.post_process_ref_install(ref_install)

    def test_post_process_ref_install_valid_scenario(self):
        ref_install = obj_lead.post_process_ref_install(ref_install)

class TestUpdateStretegicAccount:
    @pytest.mark.parametrize(
        "ref_install",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_update_strategic_account_err(self, ref_install):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.update_strategic_account(ref_install)

class TestPostProcessOutputiLead:
    @pytest.mark.parametrize(
        "output_ilead_df",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_post_process_output_ilead_err(self, output_ilead_df):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.post_process_output_ilead(output_ilead_df)

class TestProdMetaData:
    @pytest.mark.parametrize(
        "output_ilead_df",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_prod_meta_data_err(self, output_ilead_df):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.prod_meta_data(output_ilead_df)

class TestUpdateSTSLead:
    @pytest.mark.parametrize(
        "output_ilead_df",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_update_sts_leads_err(self, output_ilead_df):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.update_sts_leads(output_ilead_df)

class TestIdentifyLeads:
    @pytest.mark.parametrize(
        "df_bom, ref_lead_opp",
        [(None, pd.DataFrame()),
         (pd.DataFrame(), pd.DataFrame()),
         ('dcacac', pd.DataFrame()),
         ([123, 'aeda'], pd.DataFrame()),
         (1432, pd.DataFrame()),
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']}), pd.DataFrame()),
         ])
    def test_identify_leads_err1(self, df_bom, ref_lead_opp):
        with pytest.raises(Exception) as _:
            df_leads = obj_lead.identify_leads(df_bom, ref_lead_opp)

    @pytest.mark.parametrize(
        "df_bom, ref_lead_opp",
        [(pd.DataFrame(), None),
         (pd.DataFrame(), pd.DataFrame()),
         (pd.DataFrame(), 'dcacac'),
         (pd.DataFrame(), [123, 'aeda']),
         (pd.DataFrame(), 1432),
         (pd.DataFrame(), pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_identify_leads_err1(self, df_bom, ref_lead_opp):
        with pytest.raises(Exception) as _:
            df_leads = obj_lead.identify_leads(df_bom, ref_lead_opp)

class TestClassifyLeads:
    @pytest.mark.parametrize(
        "df_leads_out",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_update_sts_leads_err(self, df_leads_out):
        with pytest.raises(Exception) as _:
            df_leads_out = self.classify_lead(df_leads_out)
