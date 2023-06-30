"""
@file strategic_customer.py


@brief Identify and group serial numbers for a strategic account


@details This file uses customer name and contact domain/s of a unit to
classify if unit belongs to a strategic account holder. Logic for identifying
the strategic given in refence file provided by business which contains the
various aliases and domains for a given customer.


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.

"""

# %%***** Load Modules *****
import os
os.chdir('C:/Users/E9780837/OneDrive - Eaton/Desktop/git/ileads_lead_generation')

from utils import IO
from utils import AppLogger
from string import punctuation
import traceback
import pandas as pd


obj_io = IO()
logger = AppLogger(__name__)

# %%


class StrategicCustomer:

    def __init__(self, mode):
        self.mode = mode

        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})

        self.dict_con = {0: ['MatchType_00', 'CompanyName'],
                         1: ['MatchType_01', 'CompanyAliasName'],
                         2: ['MatchType_02', 'CompanyDomain']}

        self.ls_col_exp = ['SerialNumber', 'CompanyName',
                           'CompanyAliasName', 'CompanyDomain']

    def main_customer_list(self):

        # Read Data
        try:
            _step = 'Read data : reference'
            ref_df = self.read_ref_data()
            logger.app_success(_step)

            _step = 'Read data : Processed M2M'
            df_leads = self.read_processed_m2m()
            logger.app_success(_step)

            _step = 'Read data : Contact'
            df_contact = self.read_contact()
            logger.app_success(_step)

            # Contact data
            _step = 'Summarize Contacts'
            df_leads = self.summarize_contacts(df_contact, df_leads)
            logger.app_success(_step)

            # Identify Strategic Customers
            _step = 'Identify Strategic Customers'
            df_out = self.group_customers(ref_df, df_leads)

            # Export data
            _step = 'Export data'
            IO.write_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_results'],
                 'file_name': self.config['file']['Processed']['customer']['file_name']},
                df_out)

            return 'Successfull !'

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    # ***** Pipeline: Read *****

    def read_ref_data(self):

        try:

            # Read: Reference Data
            _step = "Read reference data"
            ref_ac_manager = IO.read_csv(
                obj_sc.mode,
                {'file_dir': obj_sc.config['file']['dir_ref'],
                 'file_name': obj_sc.config['file']['Reference']['customer'],
                 'sep': '\t'
                 }
            )

            # Post Process
            ref_ac_manager.columns = ref_ac_manager.loc[0, :]
            ref_ac_manager = ref_ac_manager.drop(0)

            ref_ac_manager = ref_ac_manager[pd.notna(
                ref_ac_manager.MatchType_01)]
            ref_ac_manager = ref_ac_manager.fillna('')
            # ls_col = ['MatchType_00', 'CompanyName', 'MatchType_01','CompanyAliasName', 'MatchType_02', 'CompanyDomain']
            # ref_df = ref_ac_manager[ls_col].copy()
            ref_ac_manager = ref_ac_manager.fillna('')

            # Format data
            for col in ref_ac_manager.columns:
                ref_ac_manager.loc[:, col] = ref_ac_manager[
                    col].astype(str).str.lower()

            logger.app_success(_step)

            return ref_ac_manager

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    def read_processed_m2m(self):

        try:

            # Read: M2M Data
            _step = "Read leads data"
            df_leads = IO.read_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_results'],
                 'file_name': self.config['file']['Processed']['processed_m2m_shipment']['file_name']}
            )

            dict_cols = {
                'Customer': 'CompanyName',
                'ShipTo_Customer': 'CompanyAliasName',
                'SerialNumber_M2M': 'SerialNumber'
            }
            df_leads = df_leads.rename(dict_cols, axis=1)
            df_leads = df_leads.loc[
                :, ['SerialNumber', 'CompanyName', 'CompanyAliasName']]

            df_leads['CompanyName'] = df_leads['CompanyName'].apply(
                lambda x: x.lstrip(punctuation).rstrip(punctuation))
            df_leads['CompanyAliasName'] = df_leads['CompanyAliasName'].apply(
                lambda x: x.lstrip(punctuation).rstrip(punctuation))

            # Format
            for col in df_leads.columns:
                df_leads.loc[:, col] = df_leads[col].fillna(
                    '').astype(str).str.lower()
            del col

            # Sort
            df_leads = df_leads.sort_values(
                by=['CompanyName', 'CompanyAliasName'])

            logger.app_success(_step)

            return df_leads

        except Exception as e:

            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    def read_contact(self):

        try:

            # Read: M2M Data
            _step = "Read leads data"
            df_contact = IO.read_csv(
                obj_sc.mode,
                {'file_dir': obj_sc.config['file']['dir_results'],
                 'file_name': obj_sc.config['file']['Processed']['contact']['file_name']}
            )

            df_contact = df_contact.rename(columns={'Email__c': "Email"})
            df_contact = df_contact.loc[:, ['SerialNumber', 'Email']]



            return df_contact

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    def summarize_contacts(self, df_contact, df_leads):

          _step = "Summarize contacts"
          try:
              # Contact data
              df_contact = df_contact[~ pd.isna(df_contact['Email'])]


              if df_contact.shape[0] == 0:
                  df_leads['CompanyDomain'] = ""
              else:
                  df_contact = df_contact.groupby(
                      'SerialNumber')['Email'].apply(', '.join)

                  df_leads = df_leads.merge(
                      df_contact, on = "SerialNumber", how="left")

                  df_leads.rename(columns={
                      "Email": "CompanyDomain"}, inplace=True)

              return df_leads
          except Exception as e:
              logger.app_fail(_step, f"{traceback.print_exc()}")
              raise Exception from e


    # ***** Identify Customer *****

    def group_customers(self, ref_df, df_leads):

        _step = "Group customers"
        try:

            # Identify
            ref_df = ref_df.reset_index(drop=True)
            for row_ix in ref_df.index:
                #row_ix = ref_df.index[0]

                ac_info = ref_df.iloc[row_ix, 1:]
                display_name = ref_df.DisplayName[row_ix]
                flag_all, ls_col_exp = self.identify_customer(
                    df_leads, ac_info)
                df_leads['flag_all'] = flag_all

                # Output
                df_temp = df_leads.loc[df_leads['flag_all'], ls_col_exp].copy()
                df_temp['StrategicCustomer'] = display_name

                if 'df_out' not in locals():
                    df_out = df_temp.copy()
                else:
                    df_out = pd.concat([df_out, df_temp])
                del df_temp

                # Drop from org data
                df_leads = df_leads.loc[~ df_leads['flag_all'], ls_col_exp]

                if df_leads.shape[0] == 0:
                    break

                logger.app_success(
                    f"{row_ix}/{ref_df.shape[0]}: {display_name}")

            # NOT categorized customers will be tagged as customer
            df_leads['StrategicCustomer'] = 'Other'
            df_out = pd.concat([df_out, df_leads])

            return df_out

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    def identify_customer(self, df_input, ac_info):

        try:
            # df_input = df_leads.copy()
            dict_con = {0: ['MatchType_00', 'CompanyName'],
                        1: ['MatchType_01', 'CompanyAliasName'],
                        2: ['MatchType_02', 'CompanyDomain']}
            ls_col_exp = ['SerialNumber', 'CompanyName',
                          'CompanyAliasName', 'CompanyDomain']
            ls_col_out = []

            for con_ix in dict_con.keys():

                # con_ix = 0    con_ix = 1 con_ix = 2
                ls_col = dict_con[con_ix]
                n_col = f'flag_{ls_col[1]}'
                ls_col_out.append(n_col)

                if (ac_info[ls_col[0]] == '') | (ac_info[ls_col[1]] == ''):
                    df_input.loc[:, n_col] = False
                elif ac_info[ls_col[0]] == 'begins with':
                    df_input[ls_col[1]] = df_input[ls_col[1]].astype(
                        str).fillna('')

                    df_input.loc[:, n_col] = df_input[ls_col[1]].apply(
                        lambda x:
                            any(list(map(
                                lambda y:
                                    y.startswith(
                                        tuple(ac_info[ls_col[1]].split(';'))),
                                    x.split(', '))))
                    )

                elif ac_info[ls_col[0]] == 'ends with':
                    df_input.loc[:, n_col] = df_input[ls_col[1]].apply(
                        lambda x:
                            any(list(map(
                                lambda y:
                                    y.endswith(
                                        tuple(ac_info[ls_col[1]].split(';'))),
                                    str(x).split(', '))))
                    )
                elif ac_info[ls_col[0]] == 'equals':
                    df_input.loc[:, n_col] = df_input[ls_col[1]].apply(
                        lambda x: x == ac_info[ls_col[1]])
                else:
                    df_input.loc[:, n_col] = False

            df_input.loc[:, 'flag_all'] = df_input[
                ls_col_out[0]] | df_input[ls_col_out[1]] | df_input[ls_col_out[2]]

            return df_input['flag_all'], ls_col_exp
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

# %%


if __name__ == '__main__':
    obj_sc = StrategicCustomer('local')
    df_out = obj_sc.main_customer_list()

# %%
