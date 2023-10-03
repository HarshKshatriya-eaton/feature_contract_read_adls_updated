"""@file class_services_data



@brief : For DCPD business, analyze services data to be consumed by
lead generation

@details : This method implements services class data which serves as an input
to the lead generation data file. Code identifies the updated date code/install date of components based on services data.
So that current age can be calculated. Component type filtering is performed based on key filtering components.
There are two conditions which are considered for component type.
1. Only Display components can be Replaced and Upgraded.
2. Rest of all component such as BCMS, Breaker, Fans, SPD, PCB support only
replacement.

@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****
from string import punctuation
import traceback
import os
import pandas as pd
import numpy as np

path = os.getcwd()
path = os.path.join(path.split('ileads_lead_generation')[0],
                    'ileads_lead_generation')
os.chdir(path)

from utils import AppLogger
from utils import IO
from utils import Filter
from utils import Format
from utils.dcpd.class_common_srnum_ops import SearchSrnum
from utils.dcpd.class_business_logic import BusinessLogic
import utils.dcpd.class_contracts_data as ccd

# Set project path
path = os.getcwd()
path = os.path.join(path.split('ileads_lead_generation')[0],
                    'ileads_lead_generation')
os.chdir(path)

# Create instance of the class
formatObj = Format()
filterObj = Filter()
contractObj = ccd.Contract()
busLogObj = BusinessLogic()
srnumObj = SearchSrnum()
loggerObj = AppLogger(__name__)
punctuation = punctuation + ' '


class ProcessServiceIncidents:
    """
    Class implements the method for implementation of extracting serial numbers
    from the raw services data based on various conditions. It considers the
    component type as Replace / Upgrade for various parameters including
    Display, PCB, SPD, Fans, Breaker.

    """

    def __init__(self, mode='local'):
        self.mode = mode
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})

    def main_services(self):  # pragma: no cover
        """
        Main pipline for processing the service data. It invokes main function.

        :raises Exception: Collects any / all exception.
        :return: Successful if data gets processed.
        :rtype: string.

        """
        # Read configuration
        try:
            # Read raw contracts data
            _step = 'Read configuration'

            dict_config_serv = self.config

            loggerObj.app_success(_step)

            # Read raw services data
            _step = 'Read raw services data'
            # df_services_raw = ENV_.read_data('services', 'data')
            file_dir = {'file_dir': self.config['file']['dir_data'],
                        'file_name': self.config['file']['Raw']
                        ['services']['file_name']}
            df_services_raw = IO.read_csv(self.mode, file_dir)

            _step = 'Filter raw services data'

            dict_config_params = dict_config_serv['services'][
                'services_data_overall']
            upgrade_component = \
                dict_config_serv['services']['UpgradeComponents'][
                    'ComponentName']
            df_services_raw = filterObj.filter_data(
                df_services_raw, dict_config_params)

            df_services_raw = df_services_raw[df_services_raw.f_all]
            loggerObj.app_success(_step)

            # Identify Hardware Changes

            # Read raw contracts data
            _step = 'Identify hardware replacements'

            df_hardware_changes = self.pipeline_id_hardwarechanges(
                df_services_raw,
                dict_config_serv['services']['Component_replacement'],
                upgrade_component)

            loggerObj.app_success(_step)

            # Identify Serial Numbers
            # Read raw contracts data
            _step = 'Identify Serial Numbers'

            df_sr_num = self.pipeline_serial_number(
                df_services_raw,
                dict_config_serv['services']['SerialNumberColumns'])

            loggerObj.app_success(_step)

            # Merge datasets
            # Read raw contracts data
            _step = 'Finalize data'
            key_id_col = dict_config_serv['services']['KeyColumns']

            if 'ContractNumber' in df_services_raw.columns:
                df_services_raw = df_services_raw.rename(
                    key_id_col, axis=1)

            df_sr_num = self.merge_data(
                df_hardware_changes, df_services_raw, df_sr_num)
            # del df_hardware_changes, df_services_raw

            # Export intermediate results data
            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name':
                              self.config['file']['Processed']['services'][
                                  'validation']
                          }

            IO.write_csv(self.mode, output_dir, df_sr_num)

            # # Serial number validation and output data
            _step = 'Expand serial numbers'
            expand_srnumdf = contractObj.get_range_srum(df_sr_num)

            # Removing the rows with none values
            expand_srnumdf['SerialNumber'].replace('', np.nan, inplace=True)
            expand_srnumdf.dropna(subset=['SerialNumber'], inplace=True)

            loggerObj.app_success(_step)

            _step = 'Validate serial number data'

            validate_srnum = contractObj.validate_contract_install_sr_num(
                expand_srnumdf)
            # Filter rows with valid serial number
            validate_srnum = validate_srnum.loc[
                validate_srnum.flag_validinstall]

            # Drop flag_valid column
            del validate_srnum['flag_validinstall']

            # Drop Serial Number column
            del validate_srnum['SerialNumber']

            validate_srnum.rename(
                columns={'SerialNumber_Partial': 'SerialNumber'}, inplace=True)

            loggerObj.app_success(_step)

            # Export data after validation with install data.
            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_intermediate'],
                          'file_name':
                              self.config['file']['Processed']['services'][
                                  'file_name']
                          }

            IO.write_csv(self.mode, output_dir, validate_srnum)

            # Identify if sidecar or jcomm comp is present and save results to an intermediate file.
            df_serv_input = validate_srnum  # For testing purposes

            loggerObj.app_success(_step)

        except Exception as excep:

            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        return 'successfull !'

    # *** Support Code ***
    def merge_data(self, df_hardware_changes, df_services_raw, df_sr_num):
        """
        Function merges the data for three input souce fields. The primary key
        considered for merging the data is 'Id' column.

        :param df_hardware_changes: DF indetifies and segregates components based
        on type of component viz. Replace / Upgrade.
        :param df_services_raw: DF contains raw data for services.
        :param df_sr_num: DF contains extracted Serial Number data
        from fields.
        :type df_input: Pandas Dataframe.
        :type df_services_raw: Pandas Dataframe.
        :type df_sr_num: Pandas Dataframe.
        :raises Exception: Collects any / all exception.Throws ValueError
                            exception for Invalid values passed to function.
        :return df_out: Merged values for identified extracted serial numbers.
        :rtype:  Pandas Dataframe

        """

        _step = 'Merge data'
        try:

            df_out = df_hardware_changes.copy()

            # ClosedDate column changed as per input data file.
            # df_temp = df_services_raw[['Id', 'Status', 'Closed_Date']]
            df_temp = df_services_raw[['Id', 'Status', 'ClosedDate']]

            df_temp = df_temp.drop_duplicates(subset=['Id'])
            df_out = df_out.merge(df_temp, on='Id', how='left')

            del df_temp

            # Query serial numbers
            df_out = df_out.merge(df_sr_num, on='Id', how='left')

            loggerObj.app_success(_step)
        except Exception as excep:

            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep
        return df_out

    def pipeline_serial_number(self, df_data, dict_cols_srnum):
        """
        Function extracts serial number from raw services data.

        :param df_data: DF containing raw services data.
        :param dict_cols_srnum: Dictionary containing serial number columns to
        consider for processing.
        :type df_data: Pandas Dataframe.
        :type dict_cols_srnum: Python dictionary.
        :raises Exception: Collects any / all exception.Throws ValueError
                            exception for Invalid values passed to function.
        :return df_out: Serial number data.
        :rtype:  Pandas Dataframe

        """

        _step = 'Identify hardware replacements'
        try:
            df_data['empty_qty'] = 0

            ls_cols = list(dict_cols_srnum.keys())
            for key in ls_cols:
                if dict_cols_srnum[key] == "":
                    del dict_cols_srnum[key]

            df_data.rename({'Id': "ContractNumber"}, axis=1, inplace=True)

            df_out = srnumObj.search_srnum_services(
                df_data)  # , dict_cols_srnum)

            df_out = df_out.rename({"ContractNumber": 'Id'}, axis=1)
            df_out.dropna(subset=["SerialNumber"], inplace=True)

            # Export intermediate serial number data to be consumed for identifying jcomm component
            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_intermediate'],
                          'file_name':
                              self.config['file']['Processed']['services'][
                                  'serial_number_services']
                          }
            IO.write_csv(self.mode, output_dir, df_out)

            expanded_sr_num = contractObj.get_range_srum(df_out)
            expanded_sr_num['SerialNumber'].replace(
                '', np.nan, inplace=True
            )
            expanded_sr_num.dropna(subset=['SerialNumber'], inplace=True)
            validated_sr_num = contractObj.validate_contract_install_sr_num(
                expanded_sr_num
            )
            validated_sr_num = validated_sr_num.loc[
                validated_sr_num.flag_validinstall
            ]
            IO.write_csv(
                self.mode,
                {'file_dir': (
                        self.config['file']['dir_results']
                        + self.config['file']['dir_intermediate']),
                    'file_name': self.config['file']['Processed']['services'][
                        'validated_sr_num']
                }, validated_sr_num)

            loggerObj.app_debug(f"{_step}: SUCCEEDED", 1)
        except Exception as excep:

            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep
        return df_out

        # return df_out

    def pipeline_id_hardwarechanges(self, df_data, dict_filt,
                                    upgrade_component):
        """
        Function identifies and segregates the component type based on
        input params.

        :param df_data: DF containing raw services data.
        :param dict_filt: Dictionary containing component data for filtering.
        :type df_data: Pandas Dataframe.
        :type dict_filt: Python dictionary.
        :raises Exception: Collects any / all exception.Throws ValueError
                            exception for Invalid values passed to function.
        :return df_out: Dataframe containing component and type of sr nums.
        :rtype:  Pandas Dataframe

        """

        _step = 'Identify hardware replacements'
        try:
            # Initialize output for accumulating all hardware replacements.
            # Feild names as per input file.
            ls_cols_interest = [
                'Id', 'Customer_Issue_Summary__c', 'Customer_Issue__c',
                'Resolution_Summary__c', 'Resolution__c']
            df_out = pd.DataFrame()

            # Component for replacement
            for component in dict_filt:
                # component = list(dict_filt.keys())[0]

                # Check if any case for component was recorded
                comp_filters = dict_filt[component]

                df_data = filterObj.filter_data(df_data, comp_filters)

                # Update output
                if any(df_data.f_all):
                    df_data_comp = df_data.loc[df_data.f_all, ls_cols_interest]

                    if component == upgrade_component:
                        filter_disp_col = df_data_comp.Customer_Issue_Summary__c.str.contains(
                            'upgrade', case=False)
                        df_data_comp['f_upgrade'] = filter_disp_col
                    else:
                        df_data_comp['f_upgrade'] = False

                    df_data_comp['f_replace'] = (
                        df_data_comp.Customer_Issue_Summary__c.str.contains(
                            'replace', case=False))

                    df_data_comp['f_all'] = (
                            df_data_comp.f_replace | df_data_comp.f_upgrade)
                    df_data_comp = df_data_comp.loc[df_data_comp['f_all'], :]

                    df_data_comp.loc[:, 'component'] = component
                    # if upgrade gets precedance over replace
                    df_data_comp.loc[:, 'type'] = ""
                    df_data_comp.loc[df_data_comp['f_replace'],
                    'type'] = 'replace'
                    df_data_comp.loc[df_data_comp['f_upgrade'],
                    'type'] = 'upgrade'

                    df_data_comp.drop(
                        ['f_upgrade', 'f_replace', 'f_all'], axis=1,
                        inplace=True)

                    df_out = pd.concat([df_out, df_data_comp])

                    del df_data_comp

                df_data.drop(['f_all'], axis=1, inplace=True)
                loggerObj.app_debug(f"{_step}: {component}: SUCCEEDED", 1)
        except Exception as excep:

            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        return df_out

    def pipline_component_identify(self, df_services_raw=None,
                                   df_services_serialnum=None):
        """
        Function identifies if JCOMM and Sidecar fields are present in the raw services data.

        :raises Exception: Collects any / all exception.Throws ValueError
                            exception for Invalid values passed to function.
        :return : Function saves an intermediate file to the processed folder which gets processed during lead generation

        """
        _step = 'Read raw services data and perform serial number mapping'

        try:
            if df_services_raw is None or df_services_serialnum is None:
                # Read raw services data

                # Specify the file directory and path
                file_dir = {'file_dir': self.config['file']['dir_data'],
                            'file_name': self.config['file']['Raw']
                            ['services']['file_name']}
                df_services_raw = IO.read_csv(self.mode, file_dir)

                # Read corresponding serial number data file for raw services data
                file_dir = {'file_dir': self.config['file']['dir_results'] +
                                        self.config['file'][
                                            'dir_intermediate'],
                            'file_name':
                                self.config['file']['Processed']['services'][
                                    'serial_number_services']
                            }
                df_services_serialnum = IO.read_csv(self.mode, file_dir)

            # Merge serial number data with raw services data
            df_services_raw_merged = df_services_raw.merge(
                df_services_serialnum, on='Id',
                how='left')

            # Read and identify JCOMM keyword data
            df_services_jcomm = df_services_raw_merged

            # JCOMM field processing
            filter_jcomm = df_services_jcomm.Customer_Issue_Summary__c.str.contains(
                'JCOMM',
                na=False,
                case=False)
            df_services_jcomm['Has_JCOMM'] = filter_jcomm
            df_services_jcomm = df_services_jcomm.loc[
                df_services_jcomm.Has_JCOMM]
            df_jcomm_output = df_services_jcomm[
                ['Id', 'Customer_Issue_Summary__c', 'SerialNumber',
                 'Has_JCOMM', 'Qty']]

            # Sidecar field processing
            df_services_sidecar = df_services_raw_merged
            filter_sidecar = df_services_sidecar.Customer_Issue_Summary__c.str.contains(
                'Sidecar',
                na=False,
                case=False)
            df_services_sidecar['Has_Sidecar'] = filter_sidecar
            df_services_sidecar = df_services_sidecar.loc[
                df_services_sidecar.Has_Sidecar]
            df_sidecar_output = df_services_sidecar[
                ['Id', 'Customer_Issue_Summary__c', 'SerialNumber',
                 'Has_Sidecar', 'Qty']]

            # Concatenate data for Sidecar and JCOMM df
            df_out = pd.concat([df_jcomm_output, df_sidecar_output])

            # df_out = df_jcomm_output.merge(df_sidecar_output, on='Id', how='outer')
            # df_out.rename(columns={'SerialNumber_x': 'SerialNumber'}, inplace=True)
            # df_out_rename = df_out.astype({"SerialNumber": str})

            # Serial number validation - Expand serial numbers
            expand_srnumdf = contractObj.get_range_srum(df_out)

            # Removing the rows with none values
            expand_srnumdf['SerialNumber'].replace('', np.nan, inplace=True)
            expand_srnumdf.dropna(subset=['SerialNumber'], inplace=True)

            # Validate serial number data
            validate_srnum = contractObj.validate_contract_install_sr_num(
                expand_srnumdf)

            # Filter rows with valid serial number
            validate_srnum = validate_srnum.loc[
                validate_srnum.flag_validinstall]

            # Drop flag_valid column
            del validate_srnum['flag_validinstall']

            # Rename serialnumber col value to Final serial number
            validate_srnum.rename(columns={'SerialNumber': 'F_SerialNumber'},
                                  inplace=True)

            # Perform a group by operation and fetch max result (Unique key filtering)
            validate_srnum = validate_srnum.groupby('SerialNumber_Partial',
                                                    group_keys=False).max()

            # Check if jcomm and sidecar data is present
            if df_jcomm_output.shape[0] == 0:
                loggerObj.app_info("JCOMM field is not present in the data")

            if df_sidecar_output.shape[0] == 0:
                loggerObj.app_info("Sidecar field is not present in the data")

            else:
                validate_srnum.rename(
                    columns={'F_SerialNumber': 'SerialNumber'}, inplace=True)
                # Export JCOMM and Sidecar fields to intermediate file
                output_dir = {'file_dir': self.config['file']['dir_results'] +
                                          self.config[
                                              'file']['dir_intermediate'],
                              'file_name':
                                  self.config['file']['Processed']['services'][
                                      'intermediate']
                              }
                IO.write_csv(self.mode, output_dir, validate_srnum)

            loggerObj.app_success(_step)

        except Exception as excep:

            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        return validate_srnum


# %% *** Call ***


if __name__ == "__main__":
    services_obj = ProcessServiceIncidents()
    services_obj.main_services()

# %%
