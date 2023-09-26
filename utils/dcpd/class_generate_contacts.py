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
import re
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
from utils.contacts_fr_events_data_final_v2 import DataExtraction

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
            if src != 'events':
                file_dir = {
                    'file_dir': obj.config['file']['dir_data'],
                    'file_name': obj.config['file']['Raw'][src]['file_name']
                }
            else:
                file_dir = {
                    'file_dir': obj.config['file']['dir_data'],
                    'file_name': obj.config['file']['Raw'][src]['file_name'],
                    'encoding': 'cp1252'
                }

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

                # extract_data
                df_data = obj.extract_data(dict_src, df_data)

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
        if type(dict_in) != dict:
            raise TypeError(
                "Contacts class prep_data method argument dict_in is not a "
                "dictionary"
            )
        try:
            for key in dict_in:
                if key == "Company_Phone":
                    min_len = 2
                else:
                    min_len = 0

                ls_col = dict_in[key]

                if isinstance(ls_col, list):
                    n_col = 'nc_' + key
                    df_data[ls_col] = df_data[ls_col].fillna("").astype(str)

                    df_data.loc[:, n_col] = df_data[ls_col].apply(
                        lambda x:
                            '; '.join(y for y in np.unique(x)
                                      if (len(str(y)) > min_len) & pd.notna(y))
                        , axis=1)

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
        if type(src) != str:
            raise TypeError(
                "Contacts class exception_src method argument src "
                "is not a string"
            )
        if not isinstance(df_data, pd.DataFrame):
            raise TypeError(
                "Contacts class exception_src method argument df_data "
                "is not a Pandas Dataframe"
            )
        if type(dict_contact) != dict:
            raise TypeError(
                "Contacts class exception_src method argument dict_contact "
                "is not a Dictionary"
            )

        match src:
            case "services":
                # Read serial numbers
                file_dir = {
                    'file_dir': (
                        self.config['file']['dir_results']
                        + self.config['file']['dir_intermediate']),
                    'file_name': self.config['file']['Processed']['services'][
                              'validated_sr_num']
                }
                df_sr_num = IO.read_csv(self.mode, file_dir)
                del file_dir

                # Merge Data
                df_data = df_data.merge(df_sr_num, on='Id', how="left")


                # Update contact dictionary
                dict_contact['Serial Number'] = 'SerialNumber'

            case "events":
                dict_contact['Serial Number'] = 'SerialNumber'

            case "contracts":
                # Read serial numbers
                file_dir = {
                    'file_dir': self.config['file']['dir_results'] +
                    self.config['file']['dir_intermediate'],
                    'file_name':
                        self.config['file']['Processed'][src]['file_name']}

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

            case "Renewal":
                # Update contact dictionary
                dict_contact['Serial Number'] = 'SerialNumber'
            case "PM":
                # Update contact dictionary
                dict_contact['Serial Number'] = 'SerialNumber'
            case _:
                logger.app_info(f"No exception for {src}", 1)

        return df_data, dict_contact

    def extract_data(self, src, df_data):
        """
        This function extracts the Contact details from Events data
        """
        try:
            _step = "Extract contact details from Events Description"
            if type(src) != str:
                raise TypeError(
                    "Contacts class extract_data method argument dict_src "
                    "is not a string"
                )
            match src:
                case "events":
                    try:
                        usa_states = (
                            self.config['output_contacts_lead']["usa_states"]
                        )
                    except Exception as e:
                        logger.app_fail(
                            "usa_states not available in config",
                            f'{traceback.print_exc()}'
                        )
                        raise ValueError from e

                    pat_state_short = (
                            ' ' + ' | '.join(list(usa_states.keys())) + ' '
                    )
                    pat_state_long = (
                            ' ' + ' | '.join(list(usa_states.values())) + ' '
                    )
                    pat_address = str.lower(
                        '(' + pat_state_short + '|' + pat_state_long + ')')

                    data_extractor = DataExtraction()
                    df_data.Description = df_data.Description.fillna("")

                    df_data.loc[:, "contact_name"] = df_data.Description.apply(
                        lambda x: data_extractor.extract_contact_name(x))
                    df_data.loc[:, "contact"] = df_data.Description.apply(
                        lambda x: data_extractor.extract_contact_no(x))
                    df_data.loc[:, "email"] = df_data.Description.apply(
                        lambda x: data_extractor.extract_email(x))
                    df_data.loc[:, "address"] = df_data.Description.apply(
                        lambda x: data_extractor.extract_address(
                            x, pat_address
                        )
                    )
                    df_data.loc[:, "SerialNumber"] = df_data["Description"]\
                        .apply(
                        lambda x: self.serial_num(str(x))
                    )

                    df_data = df_data.explode("SerialNumber").astype(str)

                    output_dir = {
                        'file_dir': self.config['file']['dir_results'] +
                                    self.config['file'][
                                        'dir_validation'],
                        'file_name':
                            self.config['file']['Processed']['contact'][
                                'events_sr_num']
                    }

                    IO.write_csv(self.mode, output_dir, df_data)
                    # df_data = contractObj.get_range_srum(df_data)

                    df_data = df_data.loc[
                        (df_data['contact'] != df_data['SerialNumber'])
                    ]

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception from e

        return df_data

    def post_process(self, df_con):
        """
        Postprocess contacts which includes following steps.

            1. Drop rows if contacts are invalid ()
            2. Drop duplicate for serial number keeping latest contact

        :param df_con: DESCRIPTION
        :type df_con: TYPE
        :return: DESCRIPTION
        :rtype: TYPE

        """
        _step = "Post process contacts"
        try:
            df_con = self.validate_op(df_con)

            # Keep latest
            df_con = self.filter_latest(df_con)

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception from e

        return df_con

    def validate_op(self, df_con):
        """
            Drop invalid rows, Valid row would have atleast one non empty
            or nan Name / email / phone.
            :param df_con: Data from all contacts.
            :type df_con: pandas DataFrame.
            :return: df_con, Validated contact details
            :rtype: pandas DataFrame.

        """
        ls_cols_must = ["Name", "Email", "Company_Phone"]
        ls_flags = []
        df_con.loc[:, "flag_include"] = False

        min_length = {
            "Name": 2,
            "Email": 4,
            "Company_Phone": 10
        }
        for col in ls_cols_must:
            # Clean Name
            df_con.loc[:, col] = df_con[col].fillna("")
            df_con.loc[:, col] = df_con[col].apply(
                lambda x: x.rstrip('_-* ').lstrip('_-* ')
            )
            ml = min_length[col]

            # Identiy Valid entries
            n_col = f"f_{col}"
            ls_flags += [n_col]

            flag1 = pd.notna(df_con[col])
            flag2 = (df_con[col] != "")
            flag = flag1 & flag2
            df_con.loc[:, n_col] = flag

            df_con.loc[:, "flag_include"] = (
                    df_con["flag_include"] | df_con.loc[:, n_col]
            )

        df_con = df_con.loc[
            (df_con["Name"].str.len() >= min_length["Name"]) |
            (df_con["Email"].str.len() >= min_length["Email"]) |
            (df_con["Company_Phone"].str.len() >= min_length["Company_Phone"])
        ]
        df_con.rename(
            columns={'Serial Number': 'SerialNumber'},
            inplace=True
        )
        df_con = contractObj.validate_contract_install_sr_num(df_con)
        df_con = df_con.loc[df_con.flag_validinstall]
        del df_con['flag_validinstall']
        del df_con['SerialNumber']
        df_con.rename(
            columns={'SerialNumber_Partial': 'SerialNumber'},
            inplace=True
        )
        df_con = df_con[df_con["flag_include"]]
        ls_flags.append("flag_include")
        df_con.drop(columns=ls_flags, inplace=True)

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
                by=['SerialNumber', 'Source', 'Contact_Type', 'Date'],
                ascending=False
            ).drop_duplicates(
                subset=['SerialNumber', 'Source', 'Contact_Type'],
                keep="first"
            )
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception from e

        return df_contact

    def serial_num(self, col):
        pattern = self.config['output_contacts_lead']["pat_srnum_event"]
        return re.findall(pattern, col)

# %% *** Call ***


if __name__ == "__main__":
    obj = Contacts()
    df_contacts = obj.pipeline_contact()

# %%
