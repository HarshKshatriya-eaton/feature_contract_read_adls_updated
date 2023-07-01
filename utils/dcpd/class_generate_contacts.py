"""@file class_generate_contacts.py.

@brief : For DCPD business; analyze contracts data from and generate contacts to be consumed
by lead generation


@details :
    Code generates contacts from the contracts data which again consists of three data sources

    1. Contracts: has all columns except SerialNumber
    2. processed_contract: has a few cols with SerialNumber


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****
# import all required modules

import os
path = os.getcwd()
path = os.path.join(path.split('ileads_lead_generation')[0],
                    'ileads_lead_generation')
os.chdir(path)

import traceback
import pandas as pd
from utils import IO
from utils import AppLogger
from utils.format_data import Format



logger = AppLogger(__name__)



class Contacts:
    """Class will extract and process contract data and processed data."""

    def __init__(self, mode='local'):
        """Initialise environment variables, class instance and variables used
        throughout the modules."""

        # class instance
        self.format = Format()
        self.mode = mode


        # variables
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})

        # steps
        self.contact_contracts = 'generate contract'


    def generate_contacts(self,ref_data=None,sr_num_data=None):

        try:

            _step = 'Read contracts and processed contracts data'
            # ref_data = pd.read_csv('./data/contract.csv',encoding='cp1252')
            if ref_data is not None and sr_num_data is not None:
                ref_data= ref_data
                sr_num_data = sr_num_data
            else:

                ref_data = IO.read_csv(self.mode,
                                         {'file_dir': self.config['file']['dir_data'],
                                          'file_name': self.config['file']['Raw']['contracts']['file_name']
                                          })
                sr_num_data =IO.read_csv(self.mode,
                                         {'file_dir': self.config['file']['dir_intermediate'],
                                          'file_name': self.config['file']['Processed']['contracts']['file_name']
                                          })

            logger.app_success(_step)

            _step = 'merge data and do initial selection of cols and drop absolute duplicates'
            # Merge df to get serial number in raw contracts data:
            sr_contracts_data = pd.merge(ref_data, sr_num_data[['SerialNumber', 'ContractNumber']],
                                         on='ContractNumber', how='left')
            # Drop absolute duplicates
            sr_contracts_data = sr_contracts_data.drop_duplicates(subset=None, keep='first')
            sr_contracts_data.dropna(subset=['SerialNumber'], inplace=True)
            ls_req_cols = ["SerialNumber", "AccountId", "LastModifiedDate", "Address__c",
                           "City__c", "Contact_Name__c", "Country__c", "Email__c",
                           "Mobile_Phone__c", "Mobile__c", "PM_Contact__c", "PM_Email__c", "PM_Mobile__c",
                           "PM_Phone__c", "Phone__c", "Renewal_Contact__c", "Renewal_Email__c", "Renewal_Mobile__c",
                           "Renewal_Phone__c", "State__c", "Zipcode__c", "Country_c__c", "Name_of_Company__c",
                           "Phone_Number__c"]

            # select required columns in contacts data
            sr_contracts_data = sr_contracts_data[ls_req_cols]

            # sr_contracts_data.to_csv('merged_data.csv')
            logger.app_success(_step)

            _step = 'apply conditions to classify as renewal, pm or, contract and add them to a new df'

            ls_contract_cols = ["Contact_Name__c", "Email__c", "Mobile_Phone__c", "Phone_Number__c","Phone__c", "Mobile__c"]
            ls_pm_cols = ["PM_Contact__c", "PM_Email__c", "PM_Mobile__c", "PM_Phone__c"]
            ls_ren_cols = ["Renewal_Contact__c", "Renewal_Email__c", "Renewal_Mobile__c", "Renewal_Phone__c"]

            # Initialise three new cols blank cols in df
            sr_contracts_data['Source_1'] = ''
            sr_contracts_data['Source_2'] = ''
            sr_contracts_data['Source_3'] = ''

            # Apply condition for contracts, PM and renewal
            sr_contracts_data.loc[sr_contracts_data[ls_contract_cols].notnull().any(axis=1), 'Source_1'] += 'Contracts'
            sr_contracts_data.loc[sr_contracts_data[ls_pm_cols].notnull().any(axis=1), 'Source_2'] += 'PM'
            sr_contracts_data.loc[sr_contracts_data[ls_ren_cols].notnull().any(axis=1), 'Source_3'] += 'Renewal'

            # Initialise a blank df
            new_df = pd.DataFrame()

            # Append data from source as contracts
            contracts_rows = sr_contracts_data[
                sr_contracts_data['Source_1'].notnull() & (sr_contracts_data['Source_1'] != '')]
            contracts_rows['Source'] = 'Contract'
            contracts_rows = pd.DataFrame(contracts_rows)
            new_df = pd.concat([new_df, contracts_rows], ignore_index=True)

            # Append data to new_df from source as PM
            PM_rows = sr_contracts_data[sr_contracts_data['Source_2'].notnull() & (sr_contracts_data['Source_2'] != '')]
            PM_rows['Source'] = 'PM'
            PM_rows = pd.DataFrame(PM_rows)
            new_df = pd.concat([new_df, PM_rows], ignore_index=True)

            # Append data to new_df from source as Renewal
            renewal_rows = sr_contracts_data[
                sr_contracts_data['Source_3'].notnull() & (sr_contracts_data['Source_3'] != '')]
            renewal_rows['Source'] = 'Renewal'
            renewal_rows = pd.DataFrame(renewal_rows)
            new_df = pd.concat([new_df, renewal_rows], ignore_index=True)
            print(new_df.shape)
            logger.app_success(_step)

            _step = 'Concat columns with contact numbers into one column and drop NaN values'
            #concat phone number cols for different serials:
            new_df['Company_Phone_renewal'] = new_df[['Renewal_Mobile__c', 'Renewal_Phone__c']].apply(
                lambda x: ';'.join([str(val) for val in x if pd.notnull(val)]), axis=1)

            new_df['Company_Phone_PM'] = new_df[['PM_Phone__c', 'PM_Mobile__c']].apply(
                lambda x: ';'.join([str(val) for val in x if pd.notnull(val)]), axis=1)
            new_df['Company_Phone_Contract'] = new_df[
                ['Mobile_Phone__c', 'Phone_Number__c', 'Phone__c', 'Mobile__c']].apply(
                lambda x: ';'.join([str(val) for val in x if pd.notnull(val)]), axis=1)

            logger.app_success(_step)


            # Drop extra cols
            new_df = new_df.drop(['Source_1', 'Source_2', 'Source_3'], axis=1)
            new_df['LastModifiedDate'] = pd.to_datetime(new_df['LastModifiedDate'])

            _step = 'Do Formatting as per output dict'

            #do formating and append in contact_contracts and drop duplicates
            # Format contracts df
            df_contract = new_df[new_df['Source'] == 'Contract']
            df_contract['Date'] = df_contract['LastModifiedDate'].dt.date
            df_contract['time'] = df_contract['LastModifiedDate'].dt.time
            df_contract.sort_values(['Date','time'], ascending=[False,False], inplace=True)
            df_contract.drop_duplicates(subset='SerialNumber', keep='first', inplace=True)
            df_contract.reset_index(drop=True, inplace=True)
            df_contract['Zipcode__c'] = pd.to_numeric(df_contract['Zipcode__c'],errors='coerce')
            output_format = self.config['output_contacts_lead']['Contracts_dict_format']
            df_contract = self.format.format_output(df_contract, output_format)

            # Format PM df
            df_pm = new_df[new_df['Source'] == 'PM']
            df_pm['Date'] = df_pm['LastModifiedDate'].dt.date
            df_pm['time'] = df_pm['LastModifiedDate'].dt.time
            df_pm.sort_values(['Date','time'], ascending=[False,False], inplace=True)
            df_pm.drop_duplicates(subset='SerialNumber', keep='first', inplace=True)
            df_pm.reset_index(drop=True, inplace=True)
            df_pm['Zipcode__c'] = pd.to_numeric(df_pm['Zipcode__c'], errors='coerce')
            output_format = self.config['output_contacts_lead']['PM_dict_format']
            df_pm = self.format.format_output(df_pm, output_format)

            #Format Renewal df
            df_renewal = new_df[new_df['Source'] == 'Renewal']
            df_renewal['Date'] = df_renewal['LastModifiedDate'].dt.date
            df_renewal['time'] = df_renewal['LastModifiedDate'].dt.time
            df_renewal.sort_values(['Date','time'], ascending=[False,False], inplace=True)
            df_renewal.drop_duplicates(subset='SerialNumber', keep='first',inplace=True)
            df_contract.reset_index(drop=True, inplace=True)
            df_renewal['Zipcode__c'] = pd.to_numeric(df_renewal['Zipcode__c'], errors='coerce')
            output_format = self.config['output_contacts_lead']['Renewal_dict_format']
            df_renewal = self.format.format_output(df_renewal, output_format)

            logger.app_success(_step)

            #create a new df and append all three dfs
            contact_contracts = pd.DataFrame()
            contact_contracts = pd.concat([df_contract,df_pm,df_renewal])
            contact_contracts = contact_contracts.reset_index(drop=True)

            # write results for validation
            # IO.write_csv(self.mode, {'file_dir': self.config['file']['dir_results'] +
            #                                      self.config['file']['dir_validation'],
            #                          'file_name': "contact_contracts.csv"
            #                          }, contact_contracts)

            logger.app_success(self.contact_contracts)


        except Exception as excp:
            logger.app_fail(self.contact_contracts, f"{traceback.print_exc()}")
            raise ValueError from excp
        return contact_contracts
# %% *** Call ***

if __name__ == "__main__":
    obj = Contacts()
    obj.generate_contacts()

