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

    def test_lead4_exact_match(self):
        pass
