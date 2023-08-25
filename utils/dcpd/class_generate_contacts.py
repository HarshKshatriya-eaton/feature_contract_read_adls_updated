"""
@file class_generate_contacts.py.

@brief : For DCPD business; analyze contracts data and generate contacts.


@details :
    Code generates contacts from the contracts data which again consists of
    three data sources

    1. Contracts: has all columns except SerialNumber
    2. processed_contract: has a few cols with SerialNumber


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****
import os
path = os.getcwd()
path = os.path.join(
    path.split('ileads_lead_generation')[0], 'ileads_lead_generation')
os.chdir(path)

import numpy as np
import pandas as pd
import traceback

from utils import IO
from utils import AppLogger
from utils.format_data import Format
from utils.dcpd import Contract
from utils.class_iLead_contact import ilead_contact
from utils.filter_data import Filter

contractObj = Contract()
filter_ = Filter()
logger = AppLogger(__name__)

# %% Generate Contacts


class Contacts:
    """Class will extract and process contract data and processed data."""

    def __init__(self, mode='local'):
        """
        Initialise environment variables, class instance and variables.

        :param mode: DESCRIPTION, defaults to 'local'
        :type mode: sttring, optional
        """
        # class instance
        self.format = Format()
        self.mode = mode

        # variables
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})

        # steps
        self.contact_contracts = 'generate contract'

        self.TH_DATE = pd.to_datetime("01/01/1980")
        self.gc = ilead_contact(self.TH_DATE)

        # Read reference data
        _step = 'Read reference files'

        file_dir = {
            'file_dir': self.config['file']['dir_ref'],
            'file_name': self.config['file']['Reference']['contact_type']}
        ref_df = IO.read_csv(self.mode, file_dir)
        self.ref_df = self.gc.format_reference_file(ref_df)

        logger.app_success(_step)
        self.start_msg = ": STARTED"

    def pipeline_contact(self):
        """
        Pipeline for extracting contacts from all sources listed in config.

        :return: Pipeline status. Successful vs Failed.
        :rtype: string

        """
        try:
            # Generate Contact:
            _step = "Generate contact"
            df_con = self.deploy_across_sources()
            logger.app_success(_step)

            # PostProcess kdata
            df_con = self.post_process(df_con)

            # Export results
            _step = "Export results"

            IO.write_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_results'],
                 'file_name': self.config['file']['Processed'][
                     'output_iLead_contact']['file_name']
                 }, df_con)

            logger.app_success(_step)

        except Exception as excp:
            logger.app_fail(self.contact_contracts, f"{traceback.print_exc()}")
            raise ValueError from excp

        return df_con  # "Successful !"

    def deploy_across_sources(self):
        """
        Deploy generate_contacts across data bases listed in config.

        :return: Contacts from all the databases.
        :rtype: pandas DataFrame.

        """
        # Read list of databases to be used for generating contacts
        _step = "Read confirguration for contacts"
        try:
            dict_sources = obj.config['output_contacts_lead']["dict_dbs"]
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise ValueError from e

        # Generate contacts from all databases from config
        df_out = pd.DataFrame()
        for src in dict_sources:
            # src = list(dict_sources.keys())[0]
            f_analyze = dict_sources[src]

            if f_analyze == 1:
                _step = f"Generate contacts: {src}"

                try:
                    df_data = self.generate_contacts(src)
                except:
                    logger.app_fail(_step, f'{traceback.print_exc()}')
                    df_data = pd.DataFrame()

            elif f_analyze == 0:
                _step = f"Read old processed data: {src}"

                try:
                    file_dir = {
                        'file_dir': self.config['file']['dir_results'] +
                        self.config['file']['dir_intermediate'],
                        'file_name': ('processed_contacts' + src)}
                    df_data = IO.read_csv(obj.mode, file_dir)

                except:
                    logger.app_fail(_step, f'{traceback.print_exc()}')
                    df_data = pd.DataFrame()

            elif f_analyze == -1:
                logger.app_info(
                    f"Generate contact method not implemented for {src}")
                df_data = pd.DataFrame()

            else:
                logger.app_info(f"Unknown analyze method {f_analyze}")
                df_data = pd.DataFrame()

            # Concatenate output from all sources
            df_out = pd.concat([df_out, df_data])
            del df_data

        return df_out

    def generate_contacts(self, src):
        """
        Generate contacts for individual database.

        Steps:
            - Read raw data
            - Read columns mapping from config
            - Preprocess data (concat data if multiple columns are mapped and exceptions)
            - Generate contact
            - Export data to intermediate folder

        :param src: database name for which contacts are to be generated.
        :type src: string
        :return: Contacts for databases provided in input
        :rtype: pandas dataframe.

        """
        _step = f"Generate contact for {src}"

        try:
            logger.app_info(_step + self.start_msg)
            # Read raw data
            file_dir = {'file_dir': obj.config['file']['dir_data'],
                        'file_name': obj.config['file']['Raw'][src]['file_name']}
            df_data = IO.read_csv(obj.mode, file_dir)
            del file_dir

            ls_dict = [src]
            ls_dict += ["PM", "Renewal"] if src == "contracts" else []

            df_results = pd.DataFrame()
            for dict_src in ls_dict:
                #  dict_src = ls_dict[0]
                logger.app_info(dict_src, level=0)

                # dict_contact
                dict_contact = obj.config['output_contacts_lead'][dict_src]

                # exception handling
                df_data, dict_updated = obj.exception_src(
                    dict_src, df_data, dict_contact)

                # prep data
                df_data, dict_updated = obj.prep_data(df_data, dict_contact)
                del dict_contact

                # Generate contact
                df_contact = self.gc.create_contact(
                    df_data, dict_updated, dict_src,
                    f_form_date=False, ref_df_all=obj.ref_df)

                # Concat outputs
                df_results = pd.concat([df_results, df_contact])
                del df_contact

                # Postprocess data
                ls_col_drop = [(col)
                               for col in df_data.columns if col.startswith("nc_")]
                if len(ls_col_drop) > 0:
                    df_data = df_data.drop(columns=ls_col_drop)

            file_dir = {'file_dir': self.config['file']['dir_results'] +
                        self.config['file']['dir_intermediate'],
                        'file_name': ('processed_contacts' + src)}
            status = IO.write_csv(obj.mode, file_dir, df_results)
            del file_dir, status

            logger.app_success(_step)

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception from e

        return df_results

    def prep_data(self, df_data, dict_in):
        """
        Concatenate contact data if field is mapped to multiple columns.

        Corner scenarios:
            Input: ["a", "b", ""]   Output: "a, b"
            Input: ["a", "a", ""]   Output: "a"

        :param df_data: Input data frame.
        :type df_data: pandas DataFrame
        :param dict_in: disctionary mapping columns from database to contacts field excpected in output
        :type dict_in: dictionary
        :return: Processed data. concatenated column name:
            "nc_" + output field name
        :rtype: pandas DataFrame

        """
        _step = "Pre-Process data"
        try:
            for key in dict_in:
                ls_col = dict_in[key]

                if isinstance(ls_col, list):
                    n_col = 'nc_' + key

                    df_data[ls_col] = df_data[ls_col].fillna("").astype(str)

                    df_data.loc[:, n_col] = df_data[ls_col].apply(
                        lambda x:
                            '; '.join(y for y in np.unique(x)
                                      if (len(str(y)) > 2) & pd.notna(y)),
                            axis=1)

                    dict_in[key] = n_col
                else:
                    dict_in[key] = ls_col
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception from e

        return df_data, dict_in

    def exception_src(self, src, df_data, dict_contact):
        """
        Process specific to indiviidual databse.

        :param src: database name
        :type src: string
        :param df_data: Data for selected database
        :type df_data: pandas DataFrame.
        :param dict_contact: Mapping columns to output field names.
        :type dict_contact: dictionary
        :return: Processed data.
        :rtype: pandas DataFrame.

        """
        if src == "services":
            # Read serial numbers
            file_dir = {
                'file_dir': (
                    self.config['file']['dir_results']
                    + self.config['file']['dir_intermediate']),
                'file_name': self.config['file']['Processed']['services'][
                    'serial_number_services']}
            df_sr_num = IO.read_csv(self.mode, file_dir)
            del file_dir

            expanded_sr_num = contractObj.get_range_srum(df_sr_num)
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

            # Merge Data
            df_data = df_data.merge(validated_sr_num, on='Id', how="left")


            # Update contact dictionary
            dict_contact['Serial Number'] = 'SerialNumber'

        elif src == "events":
            # Read serial numbers
            file_dir = {
                'file_dir': (
                    self.config['file']['dir_results']
                    + self.config['file']['dir_intermediate']),
                'file_name': self.config['file']['Processed']['services'][
                    'serial_number_services']}
            df_sr_num = IO.read_csv(self.mode, file_dir)
            del file_dir

            expanded_sr_num = contractObj.get_range_srum(df_sr_num)
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

            # Merge Data
            df_data = df_data.merge(
                validated_sr_num , left_on='WhatId', right_on='Id', how="left"
            )

            # Update contact dictionary
            dict_contact['Serial Number'] = 'SerialNumber'

        elif src == "contracts":
            # Read serial numbers
            file_dir = {
                'file_dir': self.config['file']['dir_results'] +
                self.config['file']['dir_intermediate'],
                'file_name':
                    self.config['file']['Processed']['contracts']['file_name']}

            df_sr_num = IO.read_csv(self.mode, file_dir)
            df_sr_num = df_sr_num[['ContractNumber', 'SerialNumber']]

            del file_dir

            # Merge Data
            df_data = df_data.merge(df_sr_num, on='ContractNumber', how="left")

            # Data prep
            df_data.Zipcode__c = pd.to_numeric(
                df_data.Zipcode__c, errors="coerce")

            # Update contact dictionary
            dict_contact['Serial Number'] = 'SerialNumber'
        elif src == "Renewal":
            # Update contact dictionary
            dict_contact['Serial Number'] = 'SerialNumber'
        elif src == "PM":
            # Update contact dictionary
            dict_contact['Serial Number'] = 'SerialNumber'
        else:
            logger.app_info(f"No exception for {src}", 1)

        return df_data, dict_contact

    def post_process(self, df_con):
        """
        Postprocess contats which includes following steps.

            1. Drop rows if contacts are invalid ()
            2. Drop duplicate for serial number keeping latest contact

        :param df_con: DESCRIPTION
        :type df_con: TYPE
        :return: DESCRIPTION
        :rtype: TYPE

        """
        _step = "Post process contacts"
        try:
            # Drop invalid rows. Valid row would have atleast nont empty or nan Name / email / phone.
            ls_cols_must = ["Name",	 "Email", "Company_Phone"]
            ls_flags = []
            df_con.loc[:, "flag_include"] = False

            for col in ls_cols_must:
                # Clean Name
                df_con.loc[:, col] = df_con[col].apply(
                    lambda x: x.rstrip('_-*'))

                # Identiy Valid entries
                n_col = f"f_{col}"
                ls_flags += [n_col]
                df_con.loc[:, n_col] = pd.notna(
                    df_con[col]) & (df_con[col] != "")

                df_con.loc[:, "flag_include"] = (
                    df_con["flag_include"] | df_con.loc[:, n_col])

            df_con = df_con[df_con["flag_include"]]
            df_con.drop(columns=ls_flags, inplace=True)

            # Keep latest
            df_con = self.filter_latest(df_con)

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception from e

        return df_con

    def filter_latest(self, df_contact):
        """
        Drop duplicate contact keeping latest contact.

        :param df_contact: Data from all contacts.
        :type df_contact: pandas DataFrame.
        :return: DESCRIPTION
        :rtype: pandas DataFrame.

        """
        _step = "Filter to latest data"

        try:
            df_contact = df_contact.sort_values(
                by=['Serial Number', 'Source', 'Contact_Type', 'Date'],
                ascending=False
            ).drop_duplicates(
                subset=['Serial Number', 'Source', 'Contact_Type'],
                keep=False
            )
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception from e

        return df_contact

# %% *** Call ***


if __name__ == "__main__":
    obj = Contacts()
    df_contacts = obj.pipeline_contact()

# %%
