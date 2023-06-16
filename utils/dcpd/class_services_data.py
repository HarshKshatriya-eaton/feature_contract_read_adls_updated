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
import os

path = os.getcwd()
path = os.path.join(path.split('ileads_lead_generation')[0],
                    'ileads_lead_generation')
os.chdir(path)

import utils.dcpd.class_contracts_data as ccd
from utils.dcpd.class_business_logic import BusinessLogic
# import utils.dcpd.config_contract as config_contract
# from utils.dcpd import SerialNumber
from utils.dcpd.class_common_srnum_ops import SearchSrnum
from utils import Format
from utils import Filter
from utils import IO
from utils import AppLogger

formatObj = Format()
filterObj = Filter()
# ioObj = IO()
# serNumObj = SerialNumber()
contractObj = ccd.Contract()
busLogObj = BusinessLogic()
srnumObj = SearchSrnum()
loggerObj = AppLogger(__name__)

import numpy as np
import re
import pandas as pd
import traceback
from string import punctuation

punctuation = punctuation + ' '


# %% ***** Main *****

class ProcessServiceIncidents:
    def __init__(self, mode='local'):
        self.mode = 'local'
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        # self.pat_srnum1 = self.config['contracts']['srnum_pattern']['pat_srnum1']

    def main_services(self):
        """
        Main pipeline for contracts and renewal data.

        :raises Exception: Collects any / all exception.
        :return: message if successful.
        :rtype: string.

        """
        # Read configuration
        try:
            # Read raw contracts data
            _step = 'Read configuration'

            # TODO Check the name of file to be imported
            # self.config = IO.read_json( mode='local', config={
            #     "file_dir": './references/', "file_name": 'config_dcpd.json'})

            dict_config_serv = self.config

            loggerObj.app_success(_step)

            # Read Data Services

            # Read raw contracts data
            _step = 'Read raw services data'
            # df_services_raw = ENV_.read_data('services', 'data')
            df_services_raw = IO.read_csv(self.mode, {'file_dir': self.config['file']['dir_data'],
                                                      'file_name': self.config['file']['Raw']['services'][
                                                          'file_name']
                                                      })

            _step = 'Filter raw services data'
            # df_services_raw = ENV_.filters_.filter_data(
            #     df_services_raw, dict_config_serv['services_data_overall'])
            df_services_raw = filterObj.filter_data(df_services_raw,
                                                    dict_config_serv['services']['services_data_overall'])

            df_services_raw = df_services_raw[df_services_raw.f_all]
            loggerObj.app_success(_step)

            # Identify Hardware Changes

            # Read raw contracts data
            _step = 'Identify hardware replacements'

            df_hardware_changes = self.pipeline_id_hardwarechanges(
                df_services_raw, dict_config_serv['services']['Component_replacement'])

            loggerObj.app_success(_step)

            # Identify Serial Numbers
            # Read raw contracts data
            _step = 'Identify Serial Numbers'

            df_sr_num = self.pipeline_serial_number(
                df_services_raw, dict_config_serv['services']['SerialNumberColumns'])

            loggerObj.app_success(_step)

            # Merge datasets
            # Read raw contracts data
            _step = 'Finalize data'
            if 'ContractNumber' in df_services_raw.columns:
                df_services_raw = df_services_raw.rename(
                    {"ContractNumber": "Id"}, axis=1)

            df_sr_num = self.merge_data(
                df_hardware_changes, df_services_raw, df_sr_num)
            # del df_hardware_changes, df_services_raw

            # Filter dataframe based on component and type
            df_sr_num_filtered = df_sr_num.query("component == 'Display' & type=='upgrade'")

            # TODO Get the values for filtering and expanding the serial numbers from the contracts data output.

            validate_srnum = contractObj.pipeline_validate_srnum(df_sr_num)
            print("The results from the testVar is ",validate_srnum)
            # validate_srnum.to_csv("MyData_Ignore_Srnum_Vaslidate.csv")
            expand_srnumdf = contractObj.get_range_srum(validate_srnum)
            # expand_srnumdf.to_csv("Final_range_serialNumber.csv")

            # Removing the blank spaces

            expand_srnumdf['SerialNumber'].replace('', np.nan, inplace=True)
            expand_srnumdf.dropna(subset=['SerialNumber'], inplace=True)
            expand_srnumdf.to_csv("Final_range_serialNumber_afterSpaceRemoval.csv")

            # TODO Validate below code and remove redundant code..
            # Export data
            # ENV_.export_data(df_sr_num, 'services', 'validation')
            IO.write_csv(self.mode, {'file_dir': self.config['file']['dir_data'],
                                     'file_name': self.config['file']['Processed']['services']['validation']
                                     }, expand_srnumdf)


            # TODO - Check for exact output format.

            # dict_format = self.config['outputformats']['services']
            # # df_sr_num = ENV_.format_output(df_sr_num, dict_format)
            # df_sr_num = formatObj.format_output(df_sr_num, dict_format)
            # IO.write_csv(self.mode, {'file_dir': self.config['file']['dir_data'],
            #                          'file_name': self.config['file']['Processed']['services']['local_file']
            #                          }, df_sr_num)

            # ENV_.export_data(df_sr_num, 'services', 'output')

            loggerObj.app_success(_step)

        except Exception as e:

            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return 'successfull !'

    # *** Support Code ***
    def merge_data(self, df_hardware_changes, df_services_raw, df_sr_num):
        _step = 'merge data'
        try:

            df_out = df_hardware_changes.copy()

            # Get meta data
            # TODO Check for closedDate column. Actual is ClosedDate while earlier code uses Closed_Date
            # df_temp = df_services_raw[['Id', 'Status', 'Closed_Date']]
            df_temp = df_services_raw[['Id', 'Status', 'ClosedDate']]

            df_temp = df_temp.drop_duplicates(subset=['Id'])
            df_out = df_out.merge(df_temp, on='Id', how='left')
            print(df_out.shape)
            del df_temp

            # Query serial numbers
            df_out = df_out.merge(df_sr_num, on='Id', how='left')
            print(df_out.shape)

            loggerObj.app_success(_step)
        except Exception as e:

            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_out

    # def pipeline_serial_number(self, df_data, dict_cols_srnum):
    #     _step = 'Identify hardware replacements'
    #     try:
    #
    #         ls_cols = list(dict_cols_srnum.keys())
    #         for key in ls_cols:
    #             if dict_cols_srnum[key] == "":
    #                 del dict_cols_srnum[key]
    #
    #         df_data.rename({'Id': "ContractNumber"}, axis=1, inplace=True)
    #         # df_out = ccd.search_srnum(df_data, dict_cols_srnum)
    #         df_out = srnumObj.search_srnum_services(df_data)    #, dict_cols_srnum)
    #
    #         df_out = df_out.rename({"ContractNumber": 'Id'}, axis=1)
    #
    #         loggerObj.app_debug(f"{_step}: SUCCEEDED", 1)
    #     except Exception as e:
    #
    #         loggerObj.app_fail(_step, f'{traceback.print_exc()}')
    #         raise Exception('f"{_step}: Failed') from e
    #
    #     return df_out
    #
    #     # return df_out

    def pipeline_serial_number(self, df_data, dict_cols_srnum, src='services'):
        _step = 'Identify hardware replacements'
        try:
            df_data['empty_qty'] = 0

            ls_cols = list(dict_cols_srnum.keys())
            for key in ls_cols:
                if dict_cols_srnum[key] == "":
                    del dict_cols_srnum[key]

            df_data.rename({'Id': "ContractNumber"}, axis=1, inplace=True)
            # df_out = ccd.search_srnum(df_data, dict_cols_srnum)
            df_out = srnumObj.search_srnum_services(df_data)  # , dict_cols_srnum)

            df_out = df_out.rename({"ContractNumber": 'Id'}, axis=1)

            loggerObj.app_debug(f"{_step}: SUCCEEDED", 1)
        except Exception as e:

            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_out

    def pipeline_id_hardwarechanges(self, df_data, dict_filt):
        _step = 'Identify hardware replacements'
        try:
            # Initialize output for accumulating all hardware replacements.
            # TODO Validate the below feild names. They have been changed as formatting of data is not done at line
            ls_cols_interest = [
                'Id', 'Customer_Issue_Summary__c', 'Customer_Issue__c',
                'Resolution_Summary__c', 'Resolution__c']
            df_out = pd.DataFrame()

            # Component replacement
            for component in dict_filt:
                # component = list(dict_filt.keys())[0]

                # Check if any case for component was recorded
                comp_filters = dict_filt[component]

                # TODO Check and remove below code.
                # df_data = ENV_.filters_.filter_data(
                #     df_data, comp_filters)

                df_data = filterObj.filter_data(df_data, comp_filters)

                # Update output
                if any(df_data.f_all):
                    df_data_comp = df_data.loc[df_data.f_all, ls_cols_interest]

                    if component == 'Display':
                        filter_disp_col = df_data_comp.Customer_Issue_Summary__c.str.contains('upgrade')
                        df_data_comp['f_upgrade'] = filter_disp_col
                    else:
                        df_data_comp['f_upgrade'] = False

                    df_data_comp['f_replace'] = (
                        df_data_comp.Customer_Issue_Summary__c.str.contains('replace'))

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
                loggerObj.app_debug(f"{_step}: {component}: SUCCEEDED", 1)
        except Exception as e:

            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_out

# ____________________________________________________________________________________Updated code
# %% *** Call ***

if __name__ == "__main__":
    processObj = ProcessServiceIncidents()
    processObj.main_services()

# %%
