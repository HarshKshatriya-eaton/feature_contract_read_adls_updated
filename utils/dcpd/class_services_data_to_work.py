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
import re
import sys
import json
#path = os.getcwd()
#path = os.path.join(path.split('ileads_lead_generation')[0],'ileads_lead_generation')
#os.chdir(path)

from utils import AppLogger
from utils import IO
from utils import Filter
from utils import Format
from utils.dcpd.class_common_srnum_ops import SearchSrnum
from utils.dcpd.class_business_logic import BusinessLogic
import utils.dcpd.class_contracts_data as ccd


# Set project path
#path = os.getcwd()
#path = os.path.join(path.split('ileads_lead_generation')[0],'ileads_lead_generation')
#os.chdir(path)

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
        """ 
            Set config variable and the mode to adls
            to read and write files. 
        """
        config_dir = os.path.join(os.path.dirname(__file__), "../../config")
        config_file = os.path.join(config_dir, "config_dcpd.json")
        # Read the configuration file
        with open(config_file,'r') as config_file:
            config = json.load(config_file)
        #self.config=js.read_json(config_file)
        self.config = config
        self.mode = self.config.get("conf.env", "azure-adls")



    def main_services(self):  # pragma: no cover
        """
        Main pipline for processing the service data. It invokes main function.

        :raises Exception: Collects any / all exception.
        :return: Successful if data gets processed.
        :rtype: string.

        """
        # Read configuration
        try:
            _step = 'Read configuration'
            dict_config_serv = self.config

            loggerObj.app_success(_step)

            # Read raw services data
            _step = 'Read raw services data'
            file_dir = {'file_dir': self.config['file']['dir_data'],
                        'file_name': self.config['file']['Raw']['services']['file_name'],
                        'adls_config': self.config['file']['Raw']['adls_credentials'],
                        'adls_dir': self.config['file']['Raw']['services']}
            df_services_raw = IO.read_csv(self.mode, file_dir)

            # Get Config for filter
            _step = 'Filter raw services data'
            dict_config_params = dict_config_serv['services'][
                'services_data_overall']

            # Filter data
            df_services_raw = filterObj.filter_data(
                df_services_raw, dict_config_params)
            df_services_raw = df_services_raw[df_services_raw.f_all]
            loggerObj.app_success(_step)

            # Identify Serial Numbers for raw services data. Serial Number are unique and not a range. TODO: Harsh to confirm
            _step = 'Identify Serial Numbers'
            df_sr_num = self.pipeline_serial_number(
                df_services_raw,
                dict_config_serv['services']['SerialNumberColumns'])
            loggerObj.app_success(_step)

            # Merge datasets
            _step = 'Finalize data'
            key_id_col = dict_config_serv['services']['KeyColumns']

            if 'ContractNumber' in df_services_raw.columns:
                df_services_raw = df_services_raw.rename(
                    key_id_col, axis=1)

            df_services_raw_with_serial_num = \
                self.merge_data(df_services_raw, df_sr_num)

            # Export intermediate results data
            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name':
                              self.config['file']['Processed']['services'][
                                  'validation'],
                        'adls_config': self.config['file']['Processed']['adls_credentials'],
                          'adls_dir':self.config['file']['Processed']['services']['validation']
                          }

            IO.write_csv(self.mode, output_dir, df_services_raw_with_serial_num)

            # Identify Hardware Changes
            _step = 'Identify hardware replacements'
            upgrade_component = \
                dict_config_serv['services']['UpgradeComponents'][
                    'ComponentName']

            df_hardware_changes = self.pipeline_id_hardwarechanges(
                df_services_raw_with_serial_num,
                dict_config_serv['services']['Component_replacement'],
                upgrade_component)
            loggerObj.app_success(_step)

            output_dir = {'file_dir': self.config['file']['dir_results'] +
                            self.config['file'][
                                'dir_intermediate'],
                'file_name':
                    self.config['file']['Processed']['services'][
                        'file_name'],
                'adls_config': self.config['file']['Processed']['adls_credentials'],
                'adls_dir': self.config['file']['Processed']['services']
                }

            IO.write_csv(self.mode, output_dir, df_hardware_changes)
            loggerObj.app_info("Finished writing the contents of dataframe validate_srnum from function main_services defined inside class_services_data.py")


            # Identify if sidecar or jcomm comp is present and save results to an intermediate file.
            jcomm_sidecar_obj = self.pipeline_component_identify(df_services_raw_with_serial_num)
            loggerObj.app_info(f"The content of dataframe jcomm_sidecar_obj is \n{str(jcomm_sidecar_obj)}")

            loggerObj.app_success(_step)

        except Exception as excep:
            loggerObj.app_info(f"The exception message reported from main_services function defined inside class_services_data.py is {str(excep)}")
            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        loggerObj.app_info("Returning from function main_services defined inside class_services_data.py")
        return 'successfull !'

    # *** Support Code ***
    def merge_data(self, df_services_raw, df_sr_num):
        """
        Function merges the data for three input source fields. The primary key
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

            #df_out = df_hardware_changes.copy()
            #del df_hardware_changes
            # ClosedDate column changed as per input data file.
            # df_temp = df_services_raw[['Id', 'Status', 'Closed_Date']]
            df_temp = df_services_raw[['Id', 'Status', 'ClosedDate']]

            df_temp = df_temp.drop_duplicates(subset=['Id'])
            #df_out = df_out.merge(df_temp, on='Id', how='left')

            #del df_temp

            # Query serial numbers
            df_temp = df_temp.merge(df_sr_num, on='Id', how='left')

            loggerObj.app_success(_step)
        except Exception as excep:
            loggerObj.app_info(f"The error message is {str(excep)}")
            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        return df_temp

    def pipeline_serial_number(self, df_data, dict_cols_srnum): # pragma: no cover
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

            df_data.rename({'Id': "ContractNumber"}, axis=1,
                            inplace=True)
            df_out = srnumObj.search_srnum_services(df_data)

            df_out = df_out.rename({"ContractNumber": 'Id'}, axis=1)
            df_out.dropna(subset=["SerialNumber"], inplace=True)

            # Export intermediate serial number data to be 
            # consumed for identifying jcomm component
            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_intermediate'],
                          'file_name':
                              self.config['file']['Processed']['services'][
                                  'serial_number_services'],
                        'adls_config': self.config['file']['Processed']['adls_credentials'],
                            'adls_dir': self.config['file']['Processed']['services'][
                                  'serial_number_services']
                          }
            IO.write_csv(self.mode, output_dir, df_out)

            list_of_expanded_data_frames = []
            total_number_of_rows = len(df_out)
            i=1
            old = 0
            temp = 10000
            new = temp
            if total_number_of_rows >= 10000:
                while new <= total_number_of_rows:
                    loggerObj.app_info(f"Now calling get_range_srum method defined in class_contract_data.py with indexes {old} and {new}")
                    intermediate_expanded_temp_df = contractObj.get_range_srum(df_out.iloc[old:new])
                    list_of_expanded_data_frames.append(intermediate_expanded_temp_df)
                    loggerObj.app_info(f"Finished calling get_range_srum method defined in class_contract_data.py with indexes {old} and {new}")
                    old = new
                    i = i + 1
                    new = i*temp
            
            if (old < total_number_of_rows and new - total_number_of_rows > 0):
                loggerObj.app_info(f"Now calling get_range_srum method defined in class_contract_data.py with indexes {old} and {new}")
                intermediate_expanded_temp_df = contractObj.get_range_srum(df_out.iloc[old:new])
                list_of_expanded_data_frames.append(intermediate_expanded_temp_df)
                loggerObj.app_info(f"Finished calling get_range_srum method defined in class_contract_data.py with indexes {old} and {new}")
            
            loggerObj.app_info(f"Finished calling get_range_srum method defined in class_contracts_data.py for all rows in df_out for {total_number_of_rows}")
            
            # loggerObj.app_info(f"Before calling get_range_srum method defined in class_contracts_data.py the number of rows in df_out are {len(df_out)}")
            # expanded_sr_num = contractObj.get_range_srum(df_out.iloc[20000:40000])
            # expanded_sr_num = contractObj.get_range_srum(df_out)
            # loggerObj.app_info("Finished calling get_range_srum method defined in class_contracts_data.py")
            
            expanded_sr_num = pd.concat(list_of_expanded_data_frames)
            loggerObj.app_info(f"Concatentation of data frames from the list has been completed.\nThe contents of the data frame expanded_sr_num are {expanded_sr_num}")

            expanded_sr_num['SerialNumber'].replace(
                '', np.nan, inplace=True
            )
            expanded_sr_num.dropna(subset=['SerialNumber'], inplace=True)

            validated_sr_num = contractObj.validate_contract_install_sr_num(
                expanded_sr_num
            )
            # Filter rows with valid serial number
            validated_sr_num = validated_sr_num.loc[
                validated_sr_num.flag_validinstall
            ]
            # Drop flag_valid column
            del validated_sr_num['flag_validinstall']

            # Drop Serial Number column
            del validated_sr_num['SerialNumber']

            validated_sr_num.rename(
                 columns={'SerialNumber_Partial': 'SerialNumber'}, inplace=True)

            #Rename serialnumber col value to Final serial number
            # validated_sr_num.rename(columns={'SerialNumber': 'F_SerialNumber'},
            #                        inplace=True)

            # # Perform a group by operation and fetch max result (Unique key filtering)
            # validated_sr_num = validated_sr_num.groupby('SerialNumber_Partial',
            #                                          group_keys=False).max()
            # validated_sr_num.rename(
            #         columns={'F_SerialNumber': 'SerialNumber'}, inplace=True)

            IO.write_csv(
                self.mode,
                {'file_dir': (
                        self.config['file']['dir_results']
                        + self.config['file']['dir_intermediate']),
                    'file_name': self.config['file']['Processed']['services'][
                        'validated_sr_num'],
                    'adls_config': self.config['file']['Processed']['adls_credentials'],
                        'adls_dir': self.config['file']['Processed']['services'][
                        'validated_sr_num']
                }, validated_sr_num)
                
        except Exception as excep:
            loggerObj.app_info(f"The error message inside function pipeline_serial_number is {str(excep)}")
            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        loggerObj.app_debug(f"{_step}: SUCCEEDED", 1)
        return df_out

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
            loggerObj.app_info("Inside function pipeline_id_hardwarechanges defined in class_servivces_data.py")
            # Initialize output for accumulating all hardware replacements.
            # Feild names as per input file.
            ls_cols_interest = [
                'Id', 'Customer_Issue_Summary__c', 'Customer_Issue__c',
                'Resolution_Summary__c', 'Resolution__c', "component", "type"]
            df_out = pd.DataFrame()

            # Added on 2023-12-08
            # Split problem summary into a list of strings based on the multiple deliiters 
            # (currently only period and new line character are used) in order to prevent the 
            # code to generate false positives (i.e. if a component is installed and neither upgraded nor replaced then such a
            # row containing summary shall be discarded)
            
            # e.g Display replaces. BCMS installed. 
            # If summary are not split based on delimiter, then it would add entries with type as replace for both the components display and BCMS.
            # This update would ensure that only display is recorded.  
            df_data["list_of_strings_for_summary"] = df_data["Customer_Issue_Summary__c"].apply(
                lambda x : re.split('\.|\n', str(x)))

            # Component for replacement
            for component in dict_filt:
                # component = list(dict_filt.keys())[0]

                # Check if any case for component was recorded (this will include upgrade, replace, maintain)
                comp_filters = dict_filt[component]

                #This check was needed because components such as BCMS, Breaker, Display have two keys namely Customer_Issue_Summary__c and 
                # Customer_Issue__c, where as components such as PCB, SPD, Fan do not have key Customer_Issue__c. Hence, only if the component
                # has the key Customer_Issue__c a call to the function filter_data is made otherwise value for f_all column is set to True.
                if "Customer_Issue__c" in comp_filters.keys():
                    df_data = filterObj.filter_data(df_data, {"Customer_Issue__c" : comp_filters['Customer_Issue__c']})
                else :
                    df_data["f_all"] = True

                # Create regex pattern from config For identifying component replacement
                # the ?= in the regex would ensure that sequence of keywords "replace" and "component_name" 
                # when interchanged would still perform the pattern matching. 
                # for e.g. "Today BCMS was replaced" or "After the replacement of BCMS in the unit." the regex would return true.
                # Also the regex would return true only if the component_name and "replace" words are found. When either of these keywords
                # are present the regex would return false.
                filt_regex = ("(" + "|".join(comp_filters['Customer_Issue_Summary__c']['text_match_pattern'].split(',')) + ")")
                pat_mod = "replace"
                pat = f"(?=.*{filt_regex})(?=.*({pat_mod}))"
                # Boolean value for f_replace would be present based on the result of pattern matching with the regex.
                # The elements of the list (sentences in the summary spanning over more than one line) would be considered one-by-one
                # and the line would be matched against the regex, if a match is found then length of the result would be greater than zero otherwise 
                # it will be equal to zero. any() would return true if for at least one sentence in the summary matches with the regex.
                df_data['f_replace'] = df_data["list_of_strings_for_summary"].apply(lambda lines : any([len(re.findall(pat, " " + line.lower() + " "))>0 for line in lines]))

                # Identify if component was upgraded (only if applicable else initialize to False)
                if component in upgrade_component:
                    # Create regex pattern from confirg For identifying component replacement
                    pat_mod = "upgrade"
                    pat = f"(?=.*{filt_regex})(?=.*({pat_mod}))"
                    #Explanation for f_replace holds for populating the column f_upgrade  
                    df_data['f_upgrade'] = df_data["list_of_strings_for_summary"].apply(lambda lines : any([len(re.findall(pat, line.lower()))>0 for line in lines]))
                else:
                    df_data['f_upgrade'] = False

                #Update the column f_all in order to prevent considering the rows where the summary does not contain upgrade or replace.
                df_data["f_all"] = df_data["f_all"] & (df_data["f_replace"] | df_data["f_upgrade"])

                # Update output. Consider the row only if it has a summary where a component is found to be replaced/upgraded.
                if any(df_data.f_all):
                    #Select only those rows where f_all is 1/True
                    df_data_comp = df_data.loc[df_data.f_all, :]
                    #Set the value for all rows in component column which have f_all as 1/True
                    df_data_comp.loc[:, 'component'] = component
                    #Set the type for the row to upgrade if the row has 1/True for f_upgrade else set the type to replace (when f_upgrade is 0/false)
                    df_data_comp.loc[:, 'type'] = df_data_comp["f_upgrade"].apply(lambda x: "upgrade" if x is True else "replace")
            
                    df_out = pd.concat([df_out, df_data_comp[ls_cols_interest]])
                    del df_data_comp

                #Drop the columns f_all, f_upgrade, f_replace in order to proceed with the next iteration.
                df_data.drop(['f_all', "f_upgrade", "f_replace"], axis=1, inplace=True)
                loggerObj.app_debug(f"{_step}: {component}: SUCCEEDED", 1)
        except Exception as excep:
            loggerObj.app_info(f"The exception message reported from pipeline_id_hardwarechanges function defined inside class_services_data.py is {str(excep)}")
            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        loggerObj.app_info("Now returning from function pipeline_id_hardwarechanges defined in class_servivces_data.py")

        return df_out

    def pipeline_component_identify(self, df_services_raw=None,
                                   df_services_serialnum=None):
        """
        Function identifies if JCOMM and Sidecar fields are present in the raw services data.

        :raises Exception: Collects any / all exception.Throws ValueError
                            exception for Invalid values passed to function.
        :return : Function saves an intermediate file to the processed folder which gets processed during lead generation

        """
        _step = 'Read raw services data and perform serial number mapping'

        try:
            loggerObj.app_info("Inside the function pipline_component_identify defined inside class_services_data.py")
            if df_services_raw is None or df_services_serialnum is None:
                # Read raw services data

                # Specify the file directory and path
                # file_dir = {'file_dir': self.config['file']['dir_data'],
                #             'file_name': self.config['file']['Raw']
                #             ['services']['file_name'],
                #             'adls_config': self.config['file']['Raw']['adls_credentials'],
                #            'adls_dir': self.config['file']['Raw']['services']}
                # df_services_raw = IO.read_csv(self.mode, file_dir)

                # Read corresponding serial number data file for raw services data
                file_dir = {'file_dir': self.config['file']['dir_results'] +
                                        self.config['file'][
                                            'dir_intermediate'],
                            'file_name':
                                self.config['file']['Processed']['services'][
                                    'serial_number_services'],
                            'adls_config': self.config['file']['Processed']['adls_credentials'],
                           'adls_dir': self.config['file']['Processed']['services'][
                                    'serial_number_services']
                            }
                df_services_serialnum = IO.read_csv(self.mode, file_dir)

            # Merge serial number data with raw services data
            df_services_raw_merged = df_services_raw.merge(
                df_services_serialnum, on='Id',
                how='left')

            # Read and identify JCOMM keyword data
            df_services_jcomm = df_services_raw_merged

            try:
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
            except Exception as e:
                loggerObj.app_info(f"The message for the exception generated (JCOMM) is {str(e)}")
            
            # Sidecar field processing
            df_services_sidecar = df_services_raw_merged
            
            try:
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
            except Exception as e:
                loggerObj.app_info(f"The message for the exception generated (sidecar) is {str(e)}")
            
            # Concatenate data for Sidecar and JCOMM df
            df_out = pd.concat([df_jcomm_output, df_sidecar_output])

            # # Serial number validation - Expand serial numbers
            # loggerObj.app_info("Calling function get_range_srum defined inside class_contracts_data.py from function pipline_component_identify defined inside class_services_data.py")
            # expand_srnumdf = contractObj.get_range_srum(df_out)
            # loggerObj.app_info("Finished calling function get_range_srum defined inside class_contracts_data.py from function pipline_component_identify defined inside class_services_data.py")

            # # Removing the rows with none values
            # expand_srnumdf['SerialNumber'].replace('', np.nan, inplace=True)
            # expand_srnumdf.dropna(subset=['SerialNumber'], inplace=True)

            # # Validate serial number data
            # loggerObj.app_info("Calling function validate_contract_install_sr_num defined inside class_contracts_data.py from function pipline_component_identify defined inside class_services_data.py")
            # validate_srnum = contractObj.validate_contract_install_sr_num(
            #     expand_srnumdf)
            # loggerObj.app_info("Finished calling function validate_contract_install_sr_num defined inside class_contracts_data.py from function pipline_component_identify defined inside class_services_data.py")

            # # Filter rows with valid serial number
            # validate_srnum = validate_srnum.loc[
            #     validate_srnum.flag_validinstall]

            # # Drop flag_valid column
            # del validate_srnum['flag_validinstall']

            # # Rename serialnumber col value to Final serial number
            # validate_srnum.rename(columns={'SerialNumber': 'F_SerialNumber'},
            #                       inplace=True)

            # # Perform a group by operation and fetch max result (Unique key filtering)
            # validate_srnum = validate_srnum.groupby('SerialNumber_Partial',
            #                                         group_keys=False).max()

            # Check if jcomm and sidecar data is present
            if df_jcomm_output.shape[0] == 0:
                loggerObj.app_info("JCOMM field is not present in the data")

            if df_sidecar_output.shape[0] == 0:
                loggerObj.app_info("Sidecar field is not present in the data")

            else:
                # df_out.rename(
                #     columns={'F_SerialNumber': 'SerialNumber'}, inplace=True)
                # Export JCOMM and Sidecar fields to intermediate file
                output_dir = {'file_dir': self.config['file']['dir_results'] +
                                          self.config[
                                              'file']['dir_intermediate'],
                              'file_name':
                                  self.config['file']['Processed']['services'][
                                      'intermediate'],
                                'adls_config': self.config['file']['Processed']['adls_credentials'],
			                    'adls_dir': self.config['file']['Processed']['services'][
                                      'intermediate']
                              }
                IO.write_csv(self.mode, output_dir, df_out)

            loggerObj.app_success(_step)

        except Exception as excep:
            loggerObj.app_info(f"The exception raised inside the function pipline_component_identify in class_services_data.py is {str(excep)}")
            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        loggerObj.app_info("Now returning from function pipline_component_identify defined inside class_services_data.py")
        return df_out

    def services_raw_serial_num(self):

        _step = 'Read raw services data and expand serial numbers'

        try:
            # Read corresponding serial number data file for raw services data
            file_dir = {'file_dir': self.config['file']['dir_results'] + self.config['file'][
                'dir_intermediate'],
                        'file_name': self.config['file']['Processed']['services'][
                            'serial_number_services'],
                        'adls_config': self.config['file']['Processed']['adls_credentials'],
            'adls_dir': self.config['file']['Processed']['services']
                        }
            df_out = IO.read_csv(self.mode, file_dir)

            # Serial number validation - Expand serial numbers
            expand_srnumdf = contractObj.get_range_srum(df_out)

            # Removing the rows with none values
            expand_srnumdf['SerialNumber'].replace('', np.nan, inplace=True)
            expand_srnumdf.dropna(subset=['SerialNumber'], inplace=True)

            # Validate serial number data
            validate_srnum = contractObj.validate_contract_install_sr_num(expand_srnumdf)

            # Filter rows with valid serial number
            validate_srnum = validate_srnum.loc[validate_srnum.flag_validinstall]

            # Drop flag_valid column
            del validate_srnum['flag_validinstall']

            # Rename serialnumber col value to Final serial number
            validate_srnum.rename(columns={'SerialNumber': 'F_SerialNumber'}, inplace=True)

            # Perform a group by operation and fetch max result (Unique key filtering)
            validate_srnum = validate_srnum.groupby('SerialNumber_Partial', group_keys=False).max()

            validate_srnum.rename(columns={'F_SerialNumber': 'SerialNumber'}, inplace=True)

            # Export validated result fields to intermediate file
            output_dir = {'file_dir': self.config['file']['dir_results'] + self.config[
                'file']['dir_intermediate'],
                          'file_name': self.config['file']['Processed']['services'][
                              'sr_num_expand_raw_serv'],
                        'adls_config': self.config['file']['Processed']['adls_credentials'],
			'adls_dir': self.config['file']['Processed']['services']
                          }
            IO.write_csv(self.mode, output_dir, validate_srnum)

            loggerObj.app_success(_step)

        except Exception as excep:
            loggerObj.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        return validate_srnum

# %% *** Call ***


if __name__ == "__main__":# pragma: no cover
    services_obj = ProcessServiceIncidents()
    services_obj.main_services()

# %%
