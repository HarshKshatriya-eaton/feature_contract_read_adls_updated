"""@file strategic_customer.py


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

path = os.getcwd()
path = os.path.join(path.split('ileads_lead_generation')[0],
                    'ileads_lead_generation')
os.chdir(path)

from utils import IO
from utils import AppLogger
from string import punctuation
import traceback
import pandas as pd

obj_io = IO()
logger = AppLogger(__name__, level='Debug')


# %%


class StrategicCustomer:

    def __init__(self, mode='local'):
        self.mode = mode
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})

        self.dict_con = {0: ['MatchType_00', 'CompanyName'],
                         1: ['MatchType_01', 'CompanyAliasName'],
                         2: ['MatchType_02', 'CompanyDomain']}

        self.ls_col_exp = ['Serial_Number', 'CompanyName',
                           'CompanyAliasName', 'CompanyDomain']

    def main_customer_list(self, df_leads=None):  # pragma: no cover
        """
        Main pipeline for identifying strategic customers.

        :raises Exception: Capctures all erros
        :return: DESCRIPTION
        :rtype: TYPE

        """
        # Read Data
        try:
            _step = 'Read data : reference'
            ref_df = self.read_ref_data()
            logger.app_success(_step)

            # if df_leads is None:
            _step = 'Read data : Processed M2M'
            df_leads = self.read_processed_m2m(df_leads=df_leads)
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
            df_out = self.pipeline_identify_customers(ref_df, df_leads)
            logger.app_success(_step)

            # Export data
            _step = 'Export data'
            IO.write_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_results'],
                 'file_name': self.config['file']['Processed']['customer']['file_name']},
                df_out)

            return df_out

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    # ***** Pipeline: Read *****

    def read_ref_data(self, ref_ac_manager=None):
        """
        Read reference data.

        :raises Exception: if read reference data failed
        :return: reference data for identifying strategic customer.
        :rtype: pandas dataframe

        """

        try:

            # Read: Reference Data
            _step = "Read reference data"

            if ref_ac_manager is not None:
                ref_ac_manager = ref_ac_manager
            else:
                ref_ac_manager = IO.read_csv(
                    self.mode,
                    {'file_dir': self.config['file']['dir_ref'],
                     'file_name': self.config['file']['Reference']['customer'],
                     'sep': '\t'
                     }
                )

            if ref_ac_manager.columns[0] != "Display":
                ref_ac_manager = ref_ac_manager.reset_index()

            # Post Process
            ref_ac_manager.columns = ref_ac_manager.loc[0, :]
            ref_ac_manager = ref_ac_manager.drop(0)

            ref_ac_manager = ref_ac_manager[pd.notna(
                ref_ac_manager.MatchType_01)]
            ref_ac_manager = ref_ac_manager.fillna('')
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

    def read_processed_m2m(self, df_leads=None):
        """
        Read InstallBase data. Fields required are:
            SerialNumber, Customer and ShipToCustomer.

        :raises Exception: if read data failes.
        :return: Install base data eith columns of interest.
        :rtype: pandas dataframe

        """

        try:

            # Read: M2M Data
            _step = "Read leads data"

            if df_leads is not None:
                df_leads = df_leads
            else:
                df_leads = IO.read_csv(
                    self.mode,
                    {'file_dir': self.config['file']['dir_results'] + self.config['file'][
                        'dir_intermediate'],
                     'file_name': self.config['file']['Processed']['processed_install'][
                         'file_name']}
                )

            if 'Serial_Number' in df_leads.columns:
                dict_cols = {
                    'Customer': 'CompanyName',
                    'ShipTo_Customer': 'CompanyAliasName'
                }
            else:
                dict_cols = {
                    'Customer': 'CompanyName',
                    'ShipTo_Customer': 'CompanyAliasName',
                    'SerialNumber_M2M': 'Serial_Number'
                }

            df_leads = df_leads.rename(dict_cols, axis=1)
            df_leads = df_leads.loc[
                       :, ['Serial_Number', 'CompanyName', 'CompanyAliasName']]

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

    def read_contact(self, df_contact=None):
        """
        Read contacts data. Fields required are:
            SerialNumber, Email Id.

        :raises Exception: if read data failes.
        :return: Contacts data eith columns of interest.
        :rtype: pandas dataframe

        """

        try:

            # Read: M2M Data
            _step = "Read leads data"
            if df_contact is not None:
                df_contact = df_contact
            else:

                df_contact = IO.read_csv(
                    self.mode,
                    {'file_dir': self.config['file']['dir_results'] +
                                 self.config['file']['dir_intermediate'],
                     'file_name': self.config['file']['Processed']['contact']['file_name']}
                )

            df_contact = df_contact.rename(columns={'Email__c': "Email"})
            df_contact = df_contact.loc[:, ['Serial_Number', 'Email']]

            return df_contact

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            df_contact = pd.DataFrame(columns=['Serial_Number', 'Email'])
            return df_contact
            # raise Exception from e

    def summarize_contacts(self, df_contact, df_leads):
        """
        Concatenate all contact emails for a serial number.

        :param df_contact: Contact email address from all databased for a
        serial number
        :type df_contact: pandas DataFrame.
        :param df_leads: Customer and ShipTo customer name for a serial number.
        :type df_leads: pandas DataFrame.
        :raises Exception: catches all exceptions
        :return: Leads with all Company Emails conacetnated in string.
        :rtype: pandas DataFrame.

        """

        _step = "Summarize contacts"
        try:
            # Contact data
            df_contact = df_contact[~ pd.isna(df_contact['Email'])]

            if df_contact.shape[0] == 0:
                df_leads['CompanyDomain'] = ""
            else:
                df_contact = df_contact.groupby(
                    'Serial_Number')['Email'].apply(', '.join)

                df_leads = df_leads.merge(
                    df_contact, on="Serial_Number", how="left")

                df_leads.rename(columns={
                    "Email": "CompanyDomain"}, inplace=True)

            logger.app_debug(_step, 1)

            return df_leads
        except Exception as e:

            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    # ***** Identify Customer *****
    def pipeline_identify_customers(self, ref_df, df_leads):
        """
        Implementes customer identification logic for all strategic customers.

        :param ref_df: reference file for strategic customers identification.
        :type ref_df: pandas DataFrame.
        :param df_leads: DESCRIPTION
        :type df_leads: pandas DataFrame.
        :raises Exception: DESCRIPTION
        :return: Serial numbers with column idicating if its strategic customer.
            old method: customers not classified as strategic were called as Other.
            new method: customers not classified as strategic will be replaces by ShipTo Customer.
        :rtype: pandas DataFrame.

        """

        _step = "Group customers"
        try:
            logger.app_info("Identify Strategic Customers : STARTED")

            # Identify
            ref_df = ref_df.reset_index(drop=True)
            # Iterate the reference file for every stretegic customer detail
            for row_ix in ref_df.index:
                # row_ix = ref_df.index[0]

                # Select the row from reference file
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

                logger.app_debug(
                    f"{row_ix}/{ref_df.shape[0]}: {display_name}", 2)

            # NOT categorized customers will be tagged as customer
            df_leads['StrategicCustomer'] = 'Other'
            df_leads['StrategicCustomer_new'] = df_leads['CompanyName']

            df_out['StrategicCustomer_new'] = df_out['StrategicCustomer'].copy()
            df_out = pd.concat([df_out, df_leads])

            return df_out

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    def identify_customer(self, df_input, ac_info):
        """
        Identify if a serial number is one of the strategic customer based on
        customer name, customer alias and email domain.

        :param df_input: Data with serial number, customer name(2 sources),
        email domain of data yet to be classified as strategic customer.
        :type df_input: pandas DataFrame.
        :param ac_info: Referece data for identifying strategic customer.
        :type ac_info: pandas Series.
        :raises Exception: catches all exceptions
        :return: Flag indicating if given serial number is strategic account.
        :rtype: pandas Series.

        """

        _step = 'Identify customber'
        try:
            # df_input = df_leads.copy()
            dict_con = {0: ['MatchType_00', 'CompanyName'],
                        1: ['MatchType_01', 'CompanyAliasName'],
                        2: ['MatchType_02', 'CompanyDomain']}
            ls_col_exp = ['Serial_Number', 'CompanyName',
                          'CompanyAliasName', 'CompanyDomain']
            ls_col_out = []

            # Iterate over the different filters i.e. CompanyName
            # CompanyAliasName, CompanyDomain
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
                        # Iterate over the diffrent entries in the raw data column
                        # e.g. CompanyName is "ABC, ABC Cooperation", we
                        # iterate over both and execute the lambda function
                        lambda x:
                        any(
                            list(
                                map(
                                    # For every y we check if any single
                                    # refrence value is present
                                    # For e.g. refrence condition is begins with
                                    # "ABC; ABC Cooperation", we check if any
                                    # one of condition exists in x. To do this
                                    # we apply startswith to the tuple
                                    # (ABC, ABC Cooperation)
                                    lambda y:
                                    y.startswith(
                                        tuple(ac_info[ls_col[1]].split(';'))
                                    ),
                                    x.split(', ')
                                )
                            )
                        )
                    )
                elif ac_info[ls_col[0]] == 'ends with':
                    df_input.loc[:, n_col] = df_input[ls_col[1]].apply(
                        lambda x:
                        any(
                            list(
                                map(
                                    lambda y:
                                    y.endswith(
                                        tuple(ac_info[ls_col[1]].split(';'))
                                    ),
                                    str(x).split(', ')
                                )
                            )
                         )
                    )
                elif ac_info[ls_col[0]] == 'contains':
                    df_input.loc[:, n_col] = df_input[ls_col[1]].apply(
                        lambda x:
                        any(
                            list(
                                map(
                                    lambda y: self.check_contains(
                                        y, ac_info, ls_col
                                    ),
                                    str(x).split(', ')
                                )
                            )
                         )
                    )
                elif ac_info[ls_col[0]] == 'equals':
                    df_input.loc[:, n_col] = df_input[ls_col[1]].apply(
                        lambda x: x == ac_info[ls_col[1]])
                else:
                    df_input.loc[:, n_col] = False

            df_input.loc[:, 'flag_all'] = df_input[
                                              ls_col_out[0]] | df_input[ls_col_out[1]] | df_input[
                                              ls_col_out[2]]

            return df_input['flag_all'], ls_col_exp

        except Exception as e:

            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    def check_contains(self, row, ac_info, ls_col):
        """
        The method checks whether the row contains any of the keywords
        provided in the AccountManagerListing file
        """
        contains_val = any(
            list(
                map(
                    lambda z:
                    z in row,
                    ac_info[ls_col[1]].split(';')
                )
            )
        )

        return contains_val
# %%


if __name__ == '__main__':
    obj_sc = StrategicCustomer('local')
    df_out = obj_sc.main_customer_list()

# %%
