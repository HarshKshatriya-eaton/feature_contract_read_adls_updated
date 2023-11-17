"""
@file class_installbase.py.

@brief This module process below M2M Data:
    -Shipment Data
    -Serial Data
    -BOM Data

@details Shipment Data process/filter based on below steps:
            - Read Data and apply filters based on configuration:
                -Identify Product by Product Class
                -Filter out Data other than ProductClass
                -Create Foreign key and drops duplicates from data
        Serial Number Data process based on below steps:
            - Validate and Decode Serial Number Data
            - Convert Serial Number to unique Serial Number
            - Create foreign key based on Serial Number
        BOM Data process based on below steps:
            - Convert Range of Serial Number to Unique Serial Number



@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****

import os
#path = os.getcwd()
#path = os.path.join(path.split('ileads_lead_generation')[0],'ileads_lead_generation')
#os.chdir(path)

import re
import traceback
from string import punctuation
from typing import Tuple
import pandas as pd

import numpy as np

from utils.dcpd.class_business_logic import BusinessLogic
from utils.dcpd.class_serial_number import SerialNumber
from utils.strategic_customer import StrategicCustomer
from utils import IO

from utils import AppLogger

logger = AppLogger(__name__)

logger.app_info('On file class_install_base')
from utils import Format
from utils import Filter

obj_srnum = SerialNumber()
obj_bus_logic = BusinessLogic()
obj_filters = Filter()
obj_format = Format()


# %%
class InstallBase:
    """This module process the M2M:Shipment Data, M2M:Serial Data, M2M BOM Data."""

    def __init__(self, mode, config) -> pd.DataFrame:
        """Initialise environment variables, class instance and
        variables used throughout the modules."""

        # Insatance of class
        self.mode = mode

        # Steps
        self.step_main_install = "main install"
        self.step_install_base = 'Process Install Base'
        self.step_serial_number = 'Query Serial Number'
        self.step_bom_data = 'Process BOM data'
        self.step_customer_number = 'Query Customer Number'
        self.step_identify_strategic_customer = 'Identify strategic customer'
        self.step_export_data = 'Export data'
        self.unsuccessful = 'unsuccessful !'

        # Variable
        self.ls_char = [' ', '-']
        self.config = config

        self.ls_priority = ['ShipTo_Country', 'SoldTo_Country']
        self.ls_cols_out = ['key_serial', 'SerialNumber', 'Product']
        self.ls_cols_ref = ['ProductClass', 'product_type', 'product_prodclass']

        # data_install = self.main_install()

        # return data_install

    def main_install(self) -> None:  # pragma: no cover
        """
        Process Data by running the M2M, Serial Number, BOM pipline to for M2M data.

        :return: processed and filtered DataFrame
        :rtype: CSV file

        """
        try:
            # Install Base
            df_install = self.pipeline_m2m()

            # Serial Number : M2M
            df_install = self.pipeline_serialnum(
                df_install, merge_type='inner')

            # BOM
            df_install = self.pipeline_bom(df_install, merge_type='left')

            # Export
            IO.write_csv(
                self.mode,
                {
                    'file_dir': self.config['file']['dir_results'] + self.config['file']['dir_intermediate'],
                    'file_name': self.config['file']['Processed']['processed_install']['file_name'],
                    'adls_config': self.config['file']['Processed']['adls_credentials'],
                    'adls_dir': self.config['file']['Processed']['processed_install']

                }, df_install)

            # Customer Name
            df_install = self.pipeline_customer(df_install)

            # df_install = env_.filters_.format_output(df_install, self.format_cols)

            filtered_data = self.filter_mtmdata(df_install)

            # Export
            IO.write_csv(
                self.mode,
                {
                    'file_dir': self.config['file']['dir_results'] + self.config['file']['dir_intermediate'],
                    'file_name': self.config['file']['Processed']['processed_install']['file_name'],
                    'adls_config': self.config['file']['Processed']['adls_credentials'],
                    'adls_dir': self.config['file']['Processed']['processed_install']
                }, filtered_data)
            logger.app_success(self.step_export_data)

        except Exception as excp:
            logger.app_fail(
                self.step_main_install, f"{traceback.print_exc()}")
            raise ValueError from excp

    #  ******************* Support Pipelines *********************
    def filter_mtmdata(self, df_install):
        """
        Filter data for output from df_install

        :return: processed and filtered DataFrame
        :rtype: Pandas df

        """

        # Identify column to be filtered
        filter_Product_M2M = df_install['Product_M2M']

        # Replace the blank results with nan
        df_install['Product_M2M'].replace('', np.nan, inplace=True)

        #  Drop results with blank column values
        df_install.dropna(subset=['Product_M2M'], inplace=True)

        # Filter out SO where status is not closed.
        listcol_filter = ['closed']
        df_install = df_install.loc[(df_install.SOStatus.isin(listcol_filter))]

        # Flag column values where flag is not in USA
        df_install = df_install[df_install.is_in_usa]

        # Filter out Reactor component from install base data
        product_m2m_filter = ['RPP', 'PDU', 'PDU - Secondary', 'STS', 'PDU - Primary']
        df_install = df_install.loc[(df_install.Product_M2M.isin(product_m2m_filter))]

        return df_install

    def pipeline_m2m(self) -> pd.DataFrame:  # pragma: no cover
        """
        Read Shipment Data and apply filters.

        raises Exception: None
        :return: df_data_install
        :rtype: pd.DataFrame
        """
        try:
            # This method will read csv data into pandas DataFrame
            df_data_install = IO.read_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_data'],
                 'file_name': self.config['file']['Raw']['M2M']['file_name'],
                 'adls_config': self.config['file']['Raw']['adls_credentials'],
                 'adls_dir': self.config['file']['Raw']['M2M']
                 }
                 )

            # Format Data
            input_format = self.config['database']['M2M']['Dictionary Format']
            column_rename = self.config['file']['Raw']['M2M']['column_rename']
            df_data_install = df_data_install.rename(column_rename, axis=1)
            df_data_install = obj_format.format_data(
                df_data_install, input_format
            )

            df_data_install = self.get_metadata(df_data_install)
            df_data_install.reset_index(drop=True, inplace=True)
            df_data_install["ST_Cust"] = df_data_install["Customer"].copy()

            # This block will prioritise 'ShipTo_Country' and 'SoldTo_Country'
            # into 'Country' column based on Null value
            df_data_install['Country'] = obj_filters.prioratized_columns(
                df_data_install[self.ls_priority], self.ls_priority)
            ls_cols = df_data_install.columns.tolist()

            # filters are applied as per configurations[config_database.json]
            df_data_install = obj_filters.filter_data(
                df_data_install, self.config['database']['M2M']['Filters'])

            df_data_install = df_data_install.rename(
                columns={'flag_Country': 'is_in_usa'})

            # Decode product
            ref_prod = IO.read_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_ref'],
                 'file_name': self.config['file']['Reference']['product_class']['file_name'],
                 'adls_config': self.config['file']['Reference']['adls_credentials'],
                 'adls_dir': self.config['file']['Reference']['product_class']
                 })

            # filters product class as per configurations[config_database.json]
            df_data_install, ls_cols = self.filter_product_class(
                ref_prod, df_data_install, ls_cols)

            # Export processed file
            IO.write_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_results'] + self.config['file'][
                    'dir_validation'],
                 'file_name': self.config['file']['Processed']['processed_m2m_shipment'][
                     'file_name'],
                 'adls_config': self.config['file']['Processed']['adls_credentials'],
                 'adls_dir': self.config['file']['Processed']['processed_m2m_shipment']
                     
                     },
                df_data_install)

            # filters key_serial column
            df_data_install = self.filter_key_serial(df_data_install, ls_cols)

            logger.app_success(self.step_install_base)

            return df_data_install

        except Exception as excp:
            logger.app_fail(
                self.step_install_base, f"{traceback.print_exc()}")
            raise ValueError from excp

    def pipeline_serialnum(self, df_data_install: pd.DataFrame,
                           merge_type: str) -> pd.DataFrame:  # pragma: no cover
        """
        Validate and Decode the Serial Numbers from data.

        :param df_data_install: Data to be filtered
        :type: pd.DataFrame
        :param merge_type: It is custom based on requirement, Default is "inner":
            "left": use only keys from left frame,
            "right": use only keys from right frame,
            "outer": use union of keys from both frames,
            "inner": use intersection of keys from both frames.
            "cross": creates the cartesian product from both frames.
        :type: str
        :raises None: None
        :return df_data_install: Data with Validated and Decoded Serial number
        :rtype: pd.DataFrame

        """
        try:
            # Read and Filter Serial Number
            df_srnum_all = self.pipeline_process_serialnum()
            df_srnum = df_data_install[['key_serial']].copy()

            # preprocess serial number before expanding
            df_srnum_range, df_srnum = self.preprocess_expand_range(
                df_srnum_all, df_srnum)

            # gets unique/repeated serial number data
            df_out, df_couldnot = obj_srnum.get_serialnumber(
                df_srnum_range['SerialNumber'], df_srnum_range['Shipper_Qty'],
                df_srnum_range['key_serial'])

            # combined expanded serial number data with original data
            df_data_install = self.combine_serialnum_data(
                df_srnum_range, df_srnum,
                df_data_install, df_out,
                df_couldnot, merge_type)

            logger.app_success(self.step_serial_number)
            return df_data_install

        except Exception as excp:
            logger.app_fail(
                self.step_serial_number, f"{traceback.print_exc()}")
            raise ValueError from excp

    def pipeline_process_serialnum(self) -> pd.DataFrame:  # pragma: no cover
        """
        Read and filter Serial Number Data.

        :raises Exception: Raised if unknown data type provided.
        :return df_srnum: Validated Serial number data
        :rtype: pd.DataFrame

        """
        try:
            # Read SerialNumber data

            df_srnum = IO.read_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_data'],
                 'file_name': self.config['file']['Raw']['SerialNumber']['file_name'],
                 'adls_config': self.config['file']['Raw']['adls_credentials'],
                 'adls_dir': self.config['file']['Raw']['SerialNumber']
                })

            # Format Data
            input_format = self.config['database']['SerialNumber']['Dictionary Format']
            df_srnum = obj_format.format_data(df_srnum, input_format)
            df_srnum.reset_index(drop=True, inplace=True)

            # # Format - Punctuation
            df_srnum = self.clean_serialnum(df_srnum)

            # Serial (Filter : Product)
            df_srnum['Product'] = obj_bus_logic.idetify_product_fr_serial(
                df_srnum['SerialNumber'])

            # Filter : Valid Serial Number
            df_srnum.loc[:, 'valid_sr'] = obj_srnum.validate_srnum(
                df_srnum['SerialNumber'])

            # Export to csv
            IO.write_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_results'] + self.config['file'][
                    'dir_validation'],
                 'file_name': self.config['file']['Processed']['processed_serialnum']['file_name'],
                 'adls_config': self.config['file']['Processed']['adls_credentials'],
                 'adls_dir': self.config['file']['Processed']['processed_serialnum']
                 }, df_srnum)

            # foreign / parent keys : Serial Number
            df_srnum = self.create_foreignkey(df_srnum)

            return df_srnum
        except Exception as excp:
            logger.app_fail(
                "filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def pipeline_customer(self, df_data_install: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
        """
        Identify strategic customer from the data.

        :param df_data_install: Data to identify strategic customers
        :type:  pd.DataFrame
        :raises None: None
        :return df_install_data: Data with strategic customers
        :rtype:  pd.DataFrame

        """
        obj_sc = StrategicCustomer('local')
        df_customer = obj_sc.main_customer_list(df_leads=df_data_install)

        # Merge customer data with shipment, serial number and BOM data
        df_install_data = self.merge_customdata(df_customer, df_data_install)

        return df_install_data

    def id_metadata(self, df_bom):  # pragma: no cover
        """
        append rating/types column to the df_bom.

        :raises Exception: Raised if unknown data type provided.
        :return df_part_rating: Added rating column to the df_bom corresponding to part number
        :rtype: pd.DataFrame

                """
        try:
            df_ref_pdi = IO.read_csv(self.mode,
                                     {'file_dir': self.config['file']['dir_ref'],
                                      'file_name': self.config['file']['Reference']['ref_sheet_pdi'],
                                      'adls_config': self.config['file']['Reference']['adls_credentials'],
                                      'adls_dir': self.config['file']['Reference']['ref_sheet_pdi']
                                        
                                      })
            # Drop duplicates in reference file
            df_ref_pdi = df_ref_pdi.drop_duplicates(subset='PartNumber_TLN_BOM', keep='first')

            # Merge df_bom and df_ref_pdi
            df_part_rating = pd.merge(df_bom, df_ref_pdi, how='left', on='PartNumber_TLN_BOM')

            logger.app_success(self.step_bom_data)
            return df_part_rating

        except Exception as excp:
            logger.app_fail(
                self.step_bom_data, f"{traceback.print_exc()}")
            raise ValueError from excp

    def id_main_breaker(self, df_data_org):
        """
        Append main breaker details to existing install base data.

        :raises Exception: Raised if unknown data type provided.
        :return df_part_rating: Added circuit breaker column to the df_bom corresponding to part number
        :rtype: pd.DataFrame

                """
        df_data = df_data_org.copy()

        ref_main_breaker = IO.read_csv(
            self.mode,
            {'file_dir': self.config['file']['dir_ref'],
             'file_name': self.config['file']['Reference']['lead_opportunities'],
             'adls_config': self.config['file']['Reference']['adls_credentials'],
             'adls_dir': self.config['file']['Reference']['lead_opportunities'] 
            })

        export_cols = self.config['install_base']['main_breaker_cols']['export_col_data']

        ref_main_breaker = ref_main_breaker.loc[
            pd.notna(ref_main_breaker['Input CB']), export_cols]

        ref_main_breaker['PartNumber_BOM_BOM'] = ref_main_breaker['PartNumber_BOM_BOM'].str.lower()

        df_data = df_data[['Job_Index', 'PartNumber_BOM_BOM']].merge(
            ref_main_breaker, on='PartNumber_BOM_BOM', how='inner')
        df_data = df_data.rename(columns={'PartNumber_BOM_BOM': 'pn_main_braker'})
        return df_data

    def pipeline_bom(self, df_install: pd.DataFrame,
                     merge_type: str) -> pd.DataFrame:  # pragma: no cover
        """
        Convert Range of Serial number to Unique Serial number.

        :param df_install: Data to be filtered
        :type: pd.DataFrame
        :param merge_type: It is custom based on requirement, Default is "inner":
            "left": use only keys from left frame,
            "right": use only keys from right frame,
            "outer": use union of keys from both frames,
            "inner": use intersection of keys from both frames.
            "cross": creates the cartesian product from both frames.
        :type: str
        :raises None: None
        :return df_install: Data with unique Serial number
        :rtype: pd.DataFrame:

        """
        try:
            # Read SerialNumber data
            df_bom = IO.read_csv(self.mode,
                                 {
                                  'file_dir': self.config['file']['dir_data'],
                                  'file_name': self.config['file']['Raw']['bom']['file_name'],
                                  'adls_config': self.config['file']['Raw']['adls_credentials'],
                                  'adls_dir': self.config['file']['Raw']['bom'],
                                  'sep': '\t'
                                
                                  }
                                 )
            # Format Data
            input_format = self.config['database']['bom']['Dictionary Format']
            df_bom = obj_format.format_data(df_bom, input_format)
            df_bom.reset_index(drop=True, inplace=True)

            # Display Part Numbers
            df_display_parts = self.id_display_parts(df_bom)

            # Main Breakers
            df_main_breaker = self.id_main_breaker(df_bom)

            # merge BOM data with shipment and serial number data
            # df_install = self.merge_bomdata(df_bom, df_install, merge_type)

            # Drop Duplicates
            # df_bom = df_bom[ls_cols]
            df_bom = df_bom[['Job_Index', 'PartNumber_TLN_BOM']]
            df_bom = df_bom.drop_duplicates(subset=['Job_Index', 'PartNumber_TLN_BOM']).reset_index(
                drop=True)
            df_bom = self.id_metadata(df_bom)

            # Merge main breaker data and display part number data
            df_bom = df_bom.merge(df_main_breaker, on='Job_Index', how='left')
            df_bom = df_bom.merge(df_display_parts, on='Job_Index', how='left')
            # df_bom = self.id_metadata(df_bom)

            # Merge Data
            df_install = df_install.merge(df_bom, on='Job_Index', how=merge_type)
            df_install.loc[:, 'PartNumber_TLN_BOM'] = df_install[
                'PartNumber_TLN_BOM'].fillna('')

            logger.app_success(self.step_bom_data)
            return df_install

        except Exception as excp:
            logger.app_fail(
                self.step_bom_data, f"{traceback.print_exc()}")
            raise ValueError from excp

    def filter_product_class(
            self, ref_prod: pd.DataFrame,
            df_data_install: pd.DataFrame, ls_cols) -> Tuple[pd.DataFrame, list]:
        """
        Filter data based on Product Class.

        :param ref_prod: Used to filter Product Class
        :type: pd.DataFrame
        :param df_data_install: Data to be filtered
        :type: pd.DataFrame
        :param ls_cols: list of columns used for filter
        :type: list
        :raises Exception: Raised if unknown data type provided.
        :return  df_data_install, ls_cols: Filtered Data, list of column name
        :rtype: tuple

        """
        try:
            ref_prod = ref_prod[self.ls_cols_ref]
            for col in self.ls_cols_ref:
                ref_prod.loc[:, col] = ref_prod[col].str.lower()

            df_data_install = df_data_install.merge(
                ref_prod, on='ProductClass', how='left')

            # Filter based on product type
            df_data_install = df_data_install[pd.notna(
                df_data_install.product_prodclass)]
            df_data_install = df_data_install[df_data_install.product_prodclass.isin(
                list(['pdu', 'rpp', 'sts']))]
            ls_cols = ls_cols + ['product_type', 'product_prodclass', 'is_in_usa']
            return df_data_install, ls_cols
        except Exception as excp:
            logger.app_fail(
                "filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def filter_key_serial(self, df_data_install, ls_cols) -> pd.DataFrame:
        """
        Create Foreign key and drops duplicate rows.

        :param df_data_install:  Data to be filtered
        :type: pd.DataFrame
        :param ls_cols: list of columns of interest
        :type: list
        :raises ValueError: raises error if unknown data type provided.
        :return: filtered data based on ls_cols.
        :rtype: pd.DataFrame.

        """
        try:
            # Filter to columns of interest
            df_data_install = df_data_install.loc[
                df_data_install['f_all'], ls_cols]

            # Key: Foreign / parent
            # M2M Shipment Data
            df_data_install['key_serial'] = df_data_install[
                ['Shipper_Index', 'ShipperItem_Index']].apply(
                lambda x: str(int(x[0])) + ':' + str(int(x[1])), axis=1)

            # M2M BOM Data
            df_data_install['key_bom'] = df_data_install['Job_Index'].str.lower()

            # Sharepoint Serial Number
            df_data_install['key_serial_shapepoint'] = df_data_install['SO'].copy()
            df_data_install['key_serial_shapepoint'] = \
                df_data_install['key_serial_shapepoint'].astype(
                    str)
            # Drop duplicates
            df_data_install = df_data_install.drop_duplicates(
                subset=['key_serial'])
            return df_data_install
        except Exception as excp:
            logger.app_fail(
                "filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def preprocess_expand_range(self, df_srnum_all, df_srnum) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Expand serial number data based on serial key.

        :param df_srnum_all:  Data to be merged
        :type: pd.DataFrame
        :param df_srnum: Serial key data
        :type: pd.Series
        :raises ValueError: raises error if unknown data type provided.
        :return: serial number range.
        :rtype: pd.DataFrame.

        """
        try:
            df_srnum = df_srnum.merge(df_srnum_all, on='key_serial', how='inner')

            df_srnum_range = df_srnum[df_srnum.Shipper_Qty > 1]
            return df_srnum_range, df_srnum
        except Exception as excp:
            logger.app_fail(
                "expand serial number range", f"{traceback.print_exc()}")
            raise ValueError from excp

    def combine_serialnum_data(self, df_srnum_range, df_srnum,
                               df_data_install, df_out, df_couldnot, merge_type) -> pd.DataFrame:
        """
        Create Foreign key and drops duplicate rows.

        :param df_srnum_range:  Data having complete serial number range
        :type: pd.DataFrame
        :param df_srnum:  Complete Serial Number Data
        :type: pd.DataFrame
        :param df_data_install:  Original Data
        :type: pd.DataFrame
        :param df_out:  Data to be filtered
        :type: pd.DataFrame
        :param df_couldnot:  Data to be filtered
        :type: pd.DataFrame
        :param merge_type: It is custom based on requirement, Default is "inner":
            "left": use only keys from left frame,
            "right": use only keys from right frame,
            "outer": use union of keys from both frames,
            "inner": use intersection of keys from both frames.
            "cross": creates the cartesian product from both frames.
        :type: str
        :raises ValueError: raises error if unknown data type provided.
        :return: serial number process data.
        :rtype: pd.DataFrame.

        """
        try:
            df_srnum_range = df_srnum_range.rename(
                columns={'SerialNumber': 'SerialNumberOrg'})
            df_out = df_out.rename(columns={'KeySerial': 'key_serial'})

            df_srnum_range = df_srnum_range.merge(
                df_out, on=['SerialNumberOrg', 'key_serial'], how='inner')

            df_srnum_range = df_srnum_range.drop_duplicates()

            df_srnum_range = df_srnum_range.loc[:, self.ls_cols_out]

            # Club data
            df_srnum = df_srnum.loc[df_srnum.Shipper_Qty == 1, self.ls_cols_out]
            df_srnum = pd.concat([df_srnum, df_srnum_range])

            df_srnum = df_srnum.rename(
                {"SerialNumber": 'SerialNumber_M2M',
                 "Product": "Product_M2M"}, axis=1)

            # Merge two tbls
            df_data_install = df_data_install.merge(
                df_srnum, on='key_serial', how=merge_type)

            df_data_install = df_data_install.drop_duplicates(
                ['Shipper_Index', 'SerialNumber_M2M'])
            return df_data_install
        except Exception as excp:
            logger.app_fail(
                "filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def merge_bomdata(self, df_bom, df_install, merge_type) -> pd.DataFrame:
        """
        Merge BOM data to shipment data.

        :param df_bom:  Data to be merged
        :type: pd.DataFrame
        :param df_install: Data to be merged
        :type: pd.DataFrame
        :param merge_type: It is custom based on requirement, Default is "inner":
            "left": use only keys from left frame,
            "right": use only keys from right frame,
            "outer": use union of keys from both frames,
            "inner": use intersection of keys from both frames.
            "cross": creates the cartesian product from both frames.
        :type: str
        :raises ValueError: raises error if unknown data type provided.
        :return: Merged bom data with shipment data.
        :rtype: pd.DataFrame.

        """
        try:
            df_bom = df_bom[['Job_Index', 'PartNumber_TLN_BOM']]
            df_bom = df_bom.drop_duplicates()

            # Merge Data
            df_install = df_install.merge(
                df_bom, on='Job_Index', how=merge_type)
            df_install.loc[:, 'PartNumber_TLN_BOM'] = df_install[
                'PartNumber_TLN_BOM'].fillna('')
            return df_install
        except Exception as excp:
            logger.app_fail(
                "filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def merge_customdata(self, df_custom, df_data_install) -> pd.DataFrame:
        """
        Merge custom data to shipment data.

        :param df_custom:  Data to be merged
        :type: pd.DataFrame
        :param df_data_install: Data to be merged
        :type: pd.DataFrame
        :raises ValueError: raises error if unknown data type provided.
        :return: Merged custom data with shipment data.
        :rtype: pd.DataFrame.

        """
        try:
            df_custom = df_custom.drop_duplicates(subset=['Serial_Number'])
            df_install_data = df_data_install.merge(
                df_custom[['Serial_Number', 'StrategicCustomer']],
                left_on='SerialNumber_M2M', right_on="Serial_Number", how='left')

            logger.app_success(self.step_identify_strategic_customer)
            return df_install_data
        except Exception as excp:
            logger.app_fail(self.step_identify_strategic_customer, f"{traceback.print_exc()}")
            raise ValueError from excp

    def clean_serialnum(self, df_srnum) -> pd.DataFrame:
        """
        Format/clean serial number data .

        :param df_srnum:  Data to be cleaned
        :type: pd.DataFrame

        :raises ValueError: raises error if unknown data type provided.
        :return: cleaned serial number data.
        :rtype: pd.DataFrame.

        """
        try:
            # Format - Duplicate Characters e.g. -- / ---
            for char in self.ls_char:
                df_srnum['SerialNumber'] = df_srnum['SerialNumber'].apply(
                    lambda x: re.sub(f'{char}+', '-', x))

            # Format - Punctuation
            df_srnum['SerialNumber'] = df_srnum['SerialNumber'].apply(
                lambda x: x.lstrip(punctuation).rstrip(punctuation))

            return df_srnum
        except Exception as excp:
            raise ValueError from excp

    def create_foreignkey(self, df_srnum) -> pd.DataFrame:
        """
        Create foreign key for serial number data.

        :param df_srnum:  Data to create foreign key
        :type: pd.DataFrame

        :raises ValueError: raises error if unknown data type provided.
        :return: serial number data with foreign key.
        :rtype: pd.DataFrame.

        """
        try:
            df_srnum = df_srnum.loc[df_srnum['valid_sr'], :]

            # foreign / parent keys : Serial Number
            df_srnum.loc[:, ['key_serial']] = df_srnum[
                ['Shipper_Index', 'ShipperItem_Index']].apply(
                lambda x: str(int(x[0])) + ':' + str(int(x[1])), axis=1)
            return df_srnum
        except Exception as excp:
            raise ValueError from excp

    def id_display_parts(self, df_data_org):
        """
        Identify display part numbers from bom.

        :param df_data_org: dataframe containing org data
        :type df_data_org: Pandas df

        """
        try:
            df_data = df_data_org.copy()

            dict_display_parts = self.config['install_base']['dict_display_parts']

            df_out = df_data[['Job_Index']].drop_duplicates()

            for col_name_out in dict_display_parts:
                # col_name_out =  list(dict_display_parts.keys())[0]

                if 'txt_search' in dict_display_parts[col_name_out].keys():
                    part_num_keys = dict_display_parts[col_name_out]['txt_search']
                    part_num_keys = [item.lower() for item in part_num_keys]

                    df_data['flag_part'] = df_data.PartNumber_BOM_BOM.apply(
                        lambda x: x.startswith(tuple(part_num_keys)))

                    # List Models
                    df_sub = df_data[df_data['flag_part']].copy()
                else:
                    df_sub = df_data


                ls_parts_of_interest = dict_display_parts[col_name_out]['PartsOfInterest']
                ls_parts_of_interest = [str.lower(txt) for txt in ls_parts_of_interest]

                df_sub_1 = df_sub.copy()
                df_sub_1['can_raise_lead'] = df_sub['PartNumber_BOM_BOM'].isin(ls_parts_of_interest)
                df_sub_1 = df_sub_1.groupby('Job_Index')['can_raise_lead'].apply(
                    sum).reset_index()
                df_sub_1 = df_sub_1.rename(
                    columns={'can_raise_lead': f'is_valid_{col_name_out.replace("pn_", "")}_lead'})

                df_sub['can_raise_lead'] = df_sub['PartNumber_BOM_BOM'].isin(
                    ls_parts_of_interest)

                df_sub = df_sub.groupby('Job_Index')['PartNumber_BOM_BOM'].apply(
                    lambda x: self.summarize_part_num(x, ls_parts_of_interest)).reset_index()

                df_sub = df_sub.rename(columns={'PartNumber_BOM_BOM': col_name_out})

                df_sub = df_sub.merge(df_sub_1, on='Job_Index', how='left')

                df_out = df_out.merge(df_sub, on='Job_Index', how='left')

            return df_out
        except Exception as e:
            logger.app_info("failed in ")
            raise Exception from e

    def summarize_part_num(self, part_list, list_of_interest):
        """
        Summarize part numbers for a JonIndex.

        :param part_list: list of PartNumbers in a product.
        :type part_list: list
        :param list_of_interest: list of part number of interest.
        :type list_of_interest: list
        :return: Output format PartNumber of interest (other PartNumbers)
        :rtype: string

        """
        if type(list_of_interest) != list:
            raise TypeError(
                "InstallBase class summarize_part_num method argument"
                " is not a list"
            )
        part_list = list(np.unique(part_list))

        ls_present = [str.upper(pn) for pn in part_list if (pn in list_of_interest)]
        other_parts = len(part_list) - len(ls_present)

        out = "" if len(ls_present)==0 else (", ".join(ls_present))
        sep = "" if len(out) == 0 else " "
        out += "" if other_parts==0 else (sep + "(# Other Parts: " + str(other_parts) + ")")

        return out

    def get_metadata(self, df_data_install):
        """
        This method calculates kva, amp, voltage metadata from description
        field.
        @param df_data_install: Dataframe containing shipment data
        @return: Updated dataframe containing metadata
        """
        try:
            kva_search_pattern = self.config['install_base']['kva_search_pattern']
            amp_search_pattern = self.config['install_base']['amp_search_pattern']
            voltage_serach_pattern = self.config['install_base']['voltage_serach_pattern']
        except ValueError as err:
            logger.app_info(
                "Failed to get required config files in get_metadata() class "
                "InstallBase"
            )
            raise Exception from err

        try:
            df_data_install["kva"] = df_data_install.Description.apply(
                lambda x: re.findall(kva_search_pattern, str(x)))
            df_data_install["amp"] = df_data_install.Description.apply(
                lambda x: re.findall(amp_search_pattern, str(x)))
            df_data_install["voltage"] = df_data_install.Description.apply(
                lambda x: re.findall(voltage_serach_pattern, str(x)))
        except Exception as err:
            logger.app_info("failed in get_metadata() class InstallBase")
            raise Exception from err

        return df_data_install



# %%