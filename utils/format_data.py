
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
# %% *** Setup Environment ***
import pandas as pd
import numpy as np

from utils import AppLogger
logger = AppLogger(__name__)

# %% *** Define Class ***


class Format:
    """Format data """

    def create_rename_dictionary(self, dict_col_dtype):
        # Pre-process dictionary
        dict_rename = {}
        for col_out in dict_col_dtype:
            col_act = dict_col_dtype[col_out]['actual_datasoure_name']
            dict_rename[col_act] = col_out
        return dict_rename

    def format_data(self, df_data, dict_col_dtype):
        """
        Prepare data for processing including renaming, format, dropna.

        :param df_data: input data to be formatted.
        :type df_data: pandas Data Frame
        :param dict_col_dtype: dictionary column renames and data types.
        Format for
        'dict_col_dtype' is given below:
        Output_Column_Name : {
            0. Original_Column_Name, 1. Data_Type, 2. bool_drop_na,
            3. input_date_type(optional), 4. output_date_type(optional))}
        :type dict_col_dtype: dictionary
        :raises ValueError: DESCRIPTION
        :return: Columns renamed and and formated data types.
        :rtype: Columns renamed and and formated data types.

        """
        logger.app_debug('Format data')

        # Pre-process dictionary
        dict_rename = self.create_rename_dictionary(dict_col_dtype)
        if False:
            dict_rename = {}
            for col_out in dict_col_dtype:
                col_act = dict_col_dtype[col_out]['actual_datasoure_name']
                dict_rename[col_act] = col_out

        # Subset dataset
        df_data = df_data.rename(columns=dict_rename)
        df_data = df_data.loc[:, list(dict_rename.values())]
        del dict_rename

        # Loop through columns
        for col in dict_col_dtype:
            # col = list(dict_col_dtype.keys())[10]
            logger.app_debug(col, 1)

            dict_val = dict_col_dtype[col]

            # Format DataTypoe
            if dict_val['data_type'] == 'date':

                # Format Date bases oon input format
                if (
                        isinstance(dict_val['input_date_format'], list) or
                        dict_val['input_date_format'] == 'list'):
                    # *** Date ***
                    logger.app_debug('Multiple formats', 2)
                    df_data[col] = self.format_date(df_data[col])

                elif dict_val['input_date_format'] == '':
                    # *** Date ***
                    logger.app_debug('No specified format', 2)
                    df_data[col] = pd.to_datetime(
                        df_data[col], errors='coerce')
                else:
                    logger.app_debug(
                        f'Format {dict_val["input_date_format"]}', 2)
                    df_data[col] = pd.to_datetime(
                        df_data[col], format=dict_val['input_date_format'],
                        errors='coerce')
                # logger.app_debug(f'{df_data.dtypes}', 1)

            elif dict_val['data_type'] == 'text':
                # *** Text ***
                df_data[col] = df_data[col].fillna("").astype(str)
                df_data[col] = df_data[col].str.lower()

            elif dict_val['data_type'] in [
                    'numeric', 'numeric : float', 'numeric : integer']:

                # *** Numeric ***
                df_data[col] = pd.to_numeric(df_data[col], errors='coerce')

                if dict_val['data_type'] == 'numeric : integer':
                    df_data[col] = df_data[col].fillna(-1).astype(int)
                    # df_data[col] = df_data[col].astype(pd.Int64Dtype())

            else:
                raise ValueError('Unknown format')

            # Drop NA
            if not dict_val['is_nullable']:
                df_data = df_data[pd.notna(df_data[col])]

        return df_data

    def format_date(self, dataset, ls_date_formats=[]):
        """
        Format date strings into pandas datetime.

        :param dataset: Date to be processed.
        :type dataset: array.
        :param ls_date_formats: Date formats in which data is entered,
        defaults to []
        :type ls_date_formats: TYPE, optional
        :return: Dates converted from date string. NAs where string cannot be converted.
        :rtype: pd.Series

        """
        data = pd.DataFrame(data={'text_org': dataset})
        data['text_org'] = data['text_org'].astype(str)
        data['text_org_upr'] = data['text_org'].str.upper()

        # Clean Dates
        data['DateClean'] = data['text_org'].copy()
        data['DateClean'] = data['DateClean'].str.strip()
        data['DateClean'] = data['DateClean'].apply(
            lambda x: x.replace('//', '-'))
        data['DateClean'] = data['DateClean'].apply(
            lambda x: x.replace('/', '-'))
        data['DateCleanWidSpace'] = data['DateClean'].copy()
        data['DateClean'] = data['DateClean'].apply(
            lambda x: x.replace(' ', '-'))

        # Initialize
        if len(ls_date_formats) == 0:
            ls_date_formats = ['%m-%d-%Y %H:%M', '%Y-%m-%d %H:%M:%S',
                               '%d-%m-%y', '%d-%m-%Y',
                               '%m-%d-%y', '%m-%d-%Y',
                               '%b-%y', '%b-%Y', '%m-%Y', '%m-%y']
        df_decision = pd.DataFrame({'DateFormat': ls_date_formats})
        df_decision['Count'] = 0
        df_decision['ColName'] = ''

        # Try all date formats
        for ix_form in range(len(ls_date_formats)):
            col_name = f'DF_{ix_form}'
            form = ls_date_formats[ix_form]
            if form in ['%m-%d-%Y %H:%M', '%Y-%m-%d %H:%M:%S']:
                data.loc[:, col_name] = data['DateCleanWidSpace'].apply(
                    lambda x: x.split(' ')[0])
                flag = data[col_name].str.len()
                data.loc[flag <= 6, col_name] = ''

                formN = form.split(' ')[0]
                data.loc[:, col_name] = pd.to_datetime(
                    data[col_name], format=formN, errors='coerce')
            else:
                data.loc[:, col_name] = pd.to_datetime(
                    data['DateClean'], format=form, errors='coerce')

            n_format = pd.notna(data[col_name]).sum()
            df_decision.loc[df_decision['DateFormat']
                            == form, 'Count'] = n_format
            df_decision.loc[df_decision['DateFormat']
                            == form, 'ColName'] = col_name

        # Prioritize Date format (based on success rate)
        df_decision = df_decision.sort_values(
            by=['Count'], ascending=False).reset_index(drop=True)

        # Finalize Date based on Priority
        data['output'] = np.nan
        data['flag'] = pd.isna(data['output'])
        for col_name in df_decision.ColName:
            data['flag'] = (pd.isna(data['output']) &
                            ~ pd.isna(data[col_name]))
            if any(data.flag):
                data.loc[data.flag, 'output'] = data.loc[data.flag,
                                                         col_name]
        logger.app_debug(f'{data["output"]}', 1)
        data['output'] = pd.to_datetime(data['output'], errors='coerce')
        return data['output']

    # Format Output
    def format_output(self, df_data, dict_form):
        """
        Format output based on dictionary.

        :param df_data: Data to be formatted.
        :type df_data: pandas DataFrame.
        :param dict_form: dictionary mapping column name to format.
        :type dict_form: dictionary.
        :return: DESCRIPTION
        :rtype: pandas dataframe

        """
        # Prep input
        df_data = df_data.reset_index(drop=True)

        # Initialize output
        df_out = pd.DataFrame(index=df_data.index)

        for col in dict_form:
            # col = list(dict_form.keys())[9]
            logger.app_debug(col, 2)

            name_ = dict_form[col]['actual_datasoure_name']
            form_ = dict_form[col]['output_format']

            # When mapped column is blank;
            # then: initialize column as empty column
            if name_ == "":
                df_out[col] = ""
                logger.app_debug(
                    f"{col}: no mapped column in dictionary", 2)
                continue

            # When mapped columns exists, but is unavailable in the data;
            # then: initialize empty column
            if name_ not in df_data.columns:
                df_out[col] = ""

                logger.app_debug(
                    f"{col}: is unavailable in input data", 2)
                continue

            # When mapped columns exists, but output format is not specified;
            # then: copy data as is
            if form_ == "":
                df_out[col] = df_data[name_].copy()
                continue

            # if mapped columns exists, and output format is  specified;
            # then: format data then copy data to
            if 'date' in form_:
                form_ = form_.replace('date : ', '')
                mapping_ = {
                    'mmmm': '%B', 'mmm': '%b', 'mm': '%m',
                    'dd': '%d',
                    'yyyy': '%Y', 'yy': '%y'}
                for key in mapping_.keys():
                    form_ = form_.replace(key, str(mapping_[key]))

                df_out[col] = pd.to_datetime(df_data[name_], errors='coerce')
                df_out[col] = df_data[name_].dt.strftime(form_)

            elif form_ == 'text : upper':
                df_out[col] = df_data[name_].str.upper()
            elif form_ == 'text : lower':
                df_out[col] = df_data[name_].str.lower()
            elif form_ == 'text : capitalize each word':
                df_out[col] = df_data[name_].str.title()
            elif 'numeric' in form_:
                if 'no_decimal' in form_:
                    form_ = 0
                else:
                    form_ = form_.replace('numeric : ', '')
                    form_ = pd.to_numeric(form_)
                df_out[col] = df_data[name_].round(decimals=form_)
            df_out[col] = df_data[name_].fillna('')


        return df_out

# %%
