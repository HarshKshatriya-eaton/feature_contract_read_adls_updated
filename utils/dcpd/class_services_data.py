
"""@file class_contracts_data



@brief : For DCPD business; analyze contracts data from SalesForce to be consumed
by lead generation


@details :
    DCPD has two tables for contracts data (contracts and renewal contracts).
    Code summaries both the datasets to understand if a unit is currently under
    active contract

    1. Contracts: has warranty and startup details

    2. Renewal contract: has contracts data (other than warranty)


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""


# %% ***** Setup Environment *****

import utils.dcpd.class_contracts_data as ccd
from utils.dcpd.class_business_logic import BusinessLogic
import utils.dcpd.config_contract as config_contract
from utils.dcpd.class_serial_number import SerialNumber

import numpy as np
import re
import pandas as pd
import traceback
from string import punctuation
punctuation = punctuation + ' '

#SRNUM = SerialNumber()
#sBL = BusinessLogic()


# %% ***** Main *****

class ProcessServiceIncoidents:
    def main_services(self):
        """
        Main pipeline for contracts and reneawal data.

        :raises Exception: Collects any / all exception.
        :return: message if successful.
        :rtype: string.

        """
        # Read configuration
        try:
            # Read raw contracts data
            _step = 'Read configuration'

            file_name = 'config_services.json'
            dict_config_serv = ENV_.read_config(file_name)

            ENV_.logger.app_success(_step)

            # Read Data Services

            # Read raw contracts data
            _step = 'Read raw services data'
            df_services_raw = ENV_.read_data('services', 'data')

            _step = 'Filter raw services data'
            df_services_raw = ENV_.filters_.filter_data(
                df_services_raw, dict_config_serv['services_data_overall'])
            df_services_raw = df_services_raw[df_services_raw.f_all]
            ENV_.logger.app_success(_step)

            # Identify Hardware Changes

            # Read raw contracts data
            _step = 'Identify hardware replacements'

            df_hardware_changes = pipeline_id_hardwarechanges(
                df_services_raw, dict_config_serv['Component_replacement'])

            ENV_.logger.app_success(_step)

            # Identify Serial Numbers
            # Read raw contracts data
            _step = 'Identify Serial Numbers'

            df_sr_num = pipeline_serial_number(
                df_services_raw, dict_config_serv['SerialNumberColumns'])

            ENV_.logger.app_success(_step)

            # Merge datasets
            # Read raw contracts data
            _step = 'Finalize data'
            if 'ContractNumber' in df_services_raw.columns:
                df_services_raw = df_services_raw.rename(
                    {"ContractNumber": "Id"}, axis=1)

            df_sr_num = merge_data(
                df_hardware_changes, df_services_raw, df_sr_num)
            #del df_hardware_changes, df_services_raw

            # Export data
            ENV_.export_data(df_sr_num, 'services', 'validation')

            dict_format = ENV_.CONF['outputformats']['services']
            df_sr_num = ENV_.format_output(df_sr_num, dict_format)

            ENV_.export_data(df_sr_num, 'services', 'output')

            ENV_.logger.app_success(_step)

        except Exception as e:

            ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return 'successfull !'


    # *** Support Code ***
    def merge_data(self, df_hardware_changes, df_services_raw, df_sr_num):
        _step = 'merge data'
        try:

            df_out = df_hardware_changes.copy()

            # Get meta data
            df_temp = df_services_raw[['Id', 'Status', 'Closed_Date']]
            df_temp = df_temp.drop_duplicates(subset=['Id'])
            df_out = df_out.merge(df_temp, on='Id', how='left')
            print(df_out.shape)
            del df_temp

            # Query serial numbers
            df_out = df_out.merge(df_sr_num, on='Id', how='left')
            print(df_out.shape)

            ENV_.logger.app_success(_step)
        except Exception as e:

            ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_out


    def pipeline_serial_number(self, df_data, dict_cols_srnum):
        _step = 'Identify hardware replacements'
        try:

            ls_cols = list(dict_cols_srnum.keys())
            for key in ls_cols:
                if dict_cols_srnum[key] == "":
                    del dict_cols_srnum[key]

            df_data.rename({'Id': "ContractNumber"}, axis=1, inplace=True)
            df_out = ccd.search_srnum(df_data, dict_cols_srnum)
            df_out = df_out.rename({"ContractNumber": 'Id'}, axis=1)

            ENV_.logger.app_debug(f"{_step}: SUCCEEDED", 1)
        except Exception as e:

            ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_out

        return df_out


    def pipeline_id_hardwarechanges(self, df_data, dict_filt):
        _step = 'Identify hardware replacements'
        try:
            # Initialize output for accumulating all hardware replacements.
            ls_cols_interest = [
                'Id', 'Customer_Issue_Summary', 'Customer_Issue',
                'Resolution_Summary', 'Resolution']
            df_out = pd.DataFrame()

            # Component replacement
            for component in dict_filt:
                # component = list(dict_filt.keys())[0]

                # Check if any case for component was recorded
                comp_filters = dict_filt[component]
                df_data = ENV_.filters_.filter_data(
                    df_data, comp_filters)

                # Update output
                if any(df_data.f_all):
                    df_data_comp = df_data.loc[df_data.f_all, ls_cols_interest]

                    if component in ['display']:
                        df_data_comp['f_upgrade'] = (
                            df_data_comp.Customer_Issue_Summary.str.contains('upgrade'))
                    else:
                        df_data_comp['f_upgrade'] = False

                    df_data_comp['f_replace'] = (
                        df_data_comp.Customer_Issue_Summary.str.contains('replace'))

                    df_data_comp['f_all'] = (
                        df_data_comp.f_replace | df_data_comp.f_upgrade)
                    df_data_comp = df_data_comp.loc[df_data_comp['f_all'], :]

                    df_data_comp.loc[:, 'component'] = component
                    # if upgrade gets precedance over replace
                    df_data_comp.loc[:, 'type'] = ""
                    df_data_comp.loc[df_data_comp['f_replace'], 'type'] = 'replace'
                    df_data_comp.loc[df_data_comp['f_upgrade'], 'type'] = 'upgrade'

                    df_data_comp.drop(
                        ['f_upgrade', 'f_replace', 'f_all'], axis=1, inplace=True)

                    df_out = pd.concat([df_out, df_data_comp])

                    del df_data_comp

                df_data.drop(['f_all'], axis=1, inplace=True)
                ENV_.logger.app_debug(f"{_step}: {component}: SUCCEEDED", 1)
        except Exception as e:

            ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_out

    # *** Serial Number ***


    def search_srnum(self, df_temp_org):
        """
        Contracts data with SerialNumbers.

        :param df_temp_org: DESCRIPTION
        :type df_temp_org: pandas DataFrame
        :raises Exception: DESCRIPTION
        :return: Contracts data with extracted SerialNumbers:

        :rtype: pandas DataFrame

        """

        _step = f"Extrat Serial Number from fields"

        try:
            df_temp = df_temp_org.copy()
            del df_temp_org

            # Input
            sep = ' '
            dict_srnum_cols = config_contract.dict_srnum_cols

            # Initialize Output
            df_serialnum = pd.DataFrame()

            # Data Type
            ls_cols = ['Qty_1__c', 'Qty_2__c', 'Qty_3__c', 'Qty_Total_del__c']
            df_temp[ls_cols] = df_temp[ls_cols].fillna(0)
            df_temp['Qty_comment'] = df_temp[ls_cols].apply(
                lambda x: x[3] - (x[0] + x[1] + x[2]), axis=1)

            # PDI Salesforce has 4 fields with SerialNumber data.
            # Extract SerialNumber data from these fields.

            for cur_field in dict_srnum_cols:
                # cur_field = list(dict_srnum_cols.keys())[0]
                cur_qty = dict_srnum_cols[cur_field]

                df_data = df_temp[[cur_field, cur_qty, 'ContractNumber']].copy()
                df_data.columns = ['SerialNumberContract', 'Qty', 'ContractNumber']

                df_data.loc[:, 'SerialNumber'] = ccd.prep_data(
                    df_data[['SerialNumberContract']], sep)

                df_data.loc[:, 'is_serialnum'] = df_data['SerialNumber'].apply(
                    lambda x: re.search(config_contract.pat_srnum1, str(x)) != None)

                # Expand Serial number
                ls_dfs = df_data.apply(lambda x: ccd.expand_srnum(
                    x, config_contract.pat_srnum), axis=1).tolist()

                # Results
                df_ls_collapse = pd.concat(ls_dfs)
                df_ls_collapse['src'] = cur_field

                df_serialnum = pd.concat([df_serialnum, df_ls_collapse])

                # ENV_.logger.app_debug(f'{cur_field}: {df_serialnum.shape[0]}')
                del ls_dfs, df_data, df_ls_collapse

            df_serialnum = df_serialnum.reset_index(drop=True)

            ENV_.logger.app_success(_step)

        except Exception as e:
            ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_serialnum


# %% *** Call ***


if __name__ == "__main__":
    main_services()

# %%
