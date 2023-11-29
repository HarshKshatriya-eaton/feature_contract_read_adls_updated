# -*- coding: utf-8 -*-

"""
@file reads data filters from configuration abd applies filters to data



@brief Configurable data processing includes fomat inpuit and ouytput data and
    filter data


@details Configurable data processing includes:
    - Format input data based on configuration setting.
    - Filter data specified in configuration for text, date and numeric fields.
    - Format output data based on configuration setting.



@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% *** Setup Environment ***
import os
import pandas as pd
import numpy as np
from datetime import datetime
import re
import json
import sys
from utils import AppLogger
logger = AppLogger(__name__)

# %% *** Define Class ***

class Filter:
    """Create Filters."""

    # *** Filter Data ***
    def filter_data(self, df_data, dict_filters):
        """
        Filter data configured.

        :param df_data: Data to be filtered.
        :type df_data: pandas dataframe.
        :param dict_filters: Filter settings in standard format described as :
            Data_source: (mandatory) Filter name.
            field_name: (mandatory) Field as it appears in input data,
            type: (mandatory) Field type can be text, date, numeric
            report: (mandatory) Allowed values yes and no
                yes: column with bool values matching filter.
                no: filter will not be reported separately. summarized filter will be named f_all
            filter related to database are optional
            text filters: text_match_exact, text_match_pattern,
                          text_match_pattern_negative, text_minimum_length,
                          text_maximum_length, text_regex
            date filters: date_min, date_max
            numeric filters: numeric_min, numeric_max, numeric_list
        :type dict_filters: dictionary
        :raises ValueError: Raised if unknown data type provided.
        :return: Data with
        :rtype: TYPE

        """
        logger.app_debug('Filter data', 1)

        df_data['f_all'] = True
        pattern = re.compile(r"\((\d+)\)")
        for col in dict_filters.keys():
            # col = list(dict_filters.keys())[0]
            logger.app_debug(col, 1)

            txt_results = pattern.findall(col)
            if len(txt_results) > 0:
                col = col.replace(f" ({txt_results[0]})", "")

            # Initialize variables
            filt_list = dict_filters[col].copy()
            report = filt_list['report']
            del filt_list['report']

            col_type = filt_list['type']
            del filt_list['type']

            # Filter Data
            if col_type == 'date':
                df_data['f_valid'] = self.validate_date(
                    df_data[col], filt_list)
            elif col_type == 'text':
                df_data['f_valid'] = self.validate_text(
                    df_data[col], filt_list)
            elif col_type == 'numeric':
                df_data['f_valid'] = self.validate_numeric(
                    df_data[col], filt_list)
            else:
                raise ValueError('Unknown filter type')

            # Action
            if report == 'no':
                df_data['f_all'] = df_data['f_all'] & df_data['f_valid']
            elif report == 'yes':
                n_col = f'flag_{col}'
                df_data[n_col] = df_data['f_valid']
            else:
                raise ValueError('Unknown filter type')

            df_data.drop(['f_valid'], axis=1, inplace=True)

        #logger.app_info("The objects along with their memory consumption in class_services_data.py are") 
        #self.check_var_size(list(locals().items()), log=True)
        return df_data

    def validate_date(self, list_date, dict_filters):
        """
        Validate dates based on specified date range.

        :param list_date: input data.
        :type list_date: list.
        :param dict_filters: Range of dates for filtering.
        Filter types are 'date_min', 'date_max'
        :type dict_filters: Dictionary
        :raises ValueError: DESCRIPTION
        :return: Flag /bool indicates if values are valid based on config.
        :rtype: pandas Series.

        """
        df_data = pd.DataFrame(data={'data': list_date})
        df_data['is_valid'] = True

        for filt_type in dict_filters:
            #   filt_type = list(ls_filters.keys())[0]
            filt_val = dict_filters[filt_type]
            filt_flag = f'f_{filt_type}'

            if 'today' in str.lower(filt_val):
                th_date = datetime.today()
            else:
                th_date = pd.to_datetime(filt_val)

            if filt_type == 'date_min':
                logger.app_debug(f'date_min: {filt_val}')
                df_data[filt_flag] = (df_data['data'] >= th_date)
            elif filt_type == 'date_max':
                logger.app_debug(f'date_max: {filt_val}')
                df_data[filt_flag] = (df_data['data'] <= th_date)
            else:
                raise ValueError('Unknown filter type')

            df_data['is_valid'] = df_data['is_valid'] & df_data[filt_flag]

        return df_data['is_valid']

    def validate_text(self, ls_text, dict_filters):
        """
        Validate dates based on specified date range.

        :param ls_text: input data.
        :type ls_text: list
        :param dict_filters: Configurable filter with date_min, date_max.
        :type dict_filters: dictionary
        :raises ValueError: raises error if unknown filter type provided.
        :return: Flag /bool indicates if values are valid based on config.
        :rtype: pandas Series.

        """
        df_text = pd.DataFrame(data={'data': ls_text})
        df_text['data'] = df_text['data'].str.lower()
        df_text['is_valid'] = True

        ls_filt_wid_list = ['text_match_exact', 'text_match_pattern',
                            'text_match_pattern_negative']
        for filt_type in dict_filters:
            #   filt_type = list(dict_filters.keys())[0]
            filt_val = dict_filters[filt_type]
            logger.app_debug(filt_type, 2)
            filt_flag = f'f_{filt_type}'

            if filt_type in ls_filt_wid_list:
                filt_val = filt_val.split(',')
                filt_val = list(map(str.lower, filt_val))

            if filt_type == 'text_match_exact':
                df_text[filt_flag] = df_text['data'].isin(filt_val)

            elif filt_type == 'text_match_pattern':
                filt_regex = ("(" + "|".join(filt_val) + ")")
                df_text[filt_flag] = df_text[
                    'data'].str.contains(filt_regex, regex=True, case=False)

            elif filt_type == 'text_match_pattern_negative':
                filt_regex = ("(" + "|".join(filt_val) + ")")
                df_text[filt_flag] = df_text[
                    'data'].str.contains(filt_regex, regex=True)
                df_text[filt_flag] = (df_text[filt_flag] == False)

            elif filt_type == 'text_minimum_length':
                df_text[filt_flag] = df_text['data'].str.len()
                df_text[filt_flag] = df_text[filt_flag] >= filt_val

            elif filt_type == 'text_maximum_length':
                df_text[filt_flag] = df_text['data'].str.len()
                df_text[filt_flag] = df_text[filt_flag] <= filt_val
            elif filt_type == 'text_regex':
                df_text[filt_flag] = df_text['data'].str.contains(
                    filt_val, regex=True)
            else:
                raise ValueError('Unknown filter type')

            df_text['is_valid'] = (df_text['is_valid'] &
                                   df_text[filt_flag])
            del filt_flag

        return df_text['is_valid']

    def validate_numeric(self, list_numebers, dict_filters):
        """
        Validate numbers based on specified filters.

        :param list_numebers: input data to be filtered.
        :type list_numebers: list.
        :param dict_filters: Filter configuration. Allowed filters are
        numeric_min, numeric_max, numeric_list
        :type dict_filters: dictionary
        :raises ValueError: raises error if unknown filter type provided.
        :return: Flag /bool indicates if values are valid based on config.
        :rtype: pandas Series.

        """
        df_data = pd.DataFrame(data={'data': list_numebers})
        df_data['is_valid'] = True

        ls_filt_wid_list = ['numeric_list']

        for filt_type in dict_filters:
            #   filt_type = list(dict_filters.keys())[0]
            filt_val = dict_filters[filt_type]
            filt_flag = f'f_{filt_type}'

            if filt_type in ls_filt_wid_list:
                if ',' in str(filt_val):
                    filt_val = filt_val.split(',')
                if not isinstance(filt_val, list):
                    filt_val = [filt_val]
                filt_val = pd.to_numeric(filt_val)

            if filt_type == 'numeric_min':
                df_data[filt_flag] = (df_data['data'] >= filt_val)
            elif filt_type == 'numeric_max':
                df_data[filt_flag] = (df_data['data'] <= filt_val)
            elif filt_type == 'numeric_list':
                df_data[filt_flag] = df_data['data'].isin(filt_val)
            else:
                raise ValueError('Unknown filter type')

            df_data['is_valid'] = df_data['is_valid'] & df_data[filt_flag]

        return df_data['is_valid']

    def prioratized_columns(self, temp_data_org, ls_priority):
        #  df_data = df_data_install.copy()
        # ls_priority = ['ShipTo_Country', 'SoldTo_Country']
        temp_data = temp_data_org.copy()
        del temp_data_org

        temp_data['out'] = np.nan

        for col in ls_priority:
            # col = ls_priority[0]
            flag_na = pd.isna(temp_data['out'])
            flag_empty = temp_data['out'] == ''
            flag_all = (flag_na | flag_empty)
            temp_data.loc[flag_all, 'out'] = temp_data.loc[flag_all, col]

        ar_out = np.array(temp_data['out'])
        return ar_out


# %%
