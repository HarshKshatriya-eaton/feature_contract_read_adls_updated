"""@file class_lead_generation.py

@brief: for DCPD business, generate the leads from reference leads data and also combine meta data
from different other sources of data i.e contract, install, services.


@details
Lead generation module works on bom data, contract data , install base data, services data
and majorly on a reference leads data as input and generates the leads for customer impact.


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
# %% Setup Environment

import os
import traceback
from string import punctuation
import re
import numpy as np
import pandas as pd

from utils.dcpd.class_business_logic import BusinessLogic
from utils.dcpd.class_serial_number import SerialNumber
from utils.strategic_customer import StrategicCustomer
from utils.format_data import Format
from utils import AppLogger
from utils import IO
from utils import Filter

path = os.getcwd()
path = os.path.join(path.split('ileads_lead_generation')[0],
                    'ileads_lead_generation')
os.chdir(path)

obj_filt = Filter()

logger = AppLogger(__name__)
punctuation = punctuation + ' '


# %% Lead Generation

class LeadGeneration:

    def __init__(self, mode='local'):
        self.mode = mode
        self.srnum = SerialNumber()
        self.bus_logic = BusinessLogic()
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        self.format = Format()

    def main_lead_generation(self):  # pragma: no cover
        """
        This is the main method or entry point for the lead generation module.
        """

        _step = 'Read Merged Contracts and Install Base data'
        try:
            df_install = self.pipeline_contract_install()
            logger.app_success(
                f"***** {df_install.SerialNumber_M2M.nunique()} *****")
            logger.app_success(_step)

            # ***** PreProcess BOM data *****
            _step = 'Process BOM data and identify leads'
            # Read Data
            df_leads = self.pipeline_bom_identify_lead(df_install)
            logger.app_success(
                f"***** {df_leads.SerialNumber_M2M.nunique()} *****")
            logger.app_success(_step)

            _step = 'Merge data: Install and BOM'
            df_leads = self.pipeline_merge(df_leads, df_install, 'meta_data')
            logger.app_success(
                f"***** {df_leads.SerialNumber_M2M.nunique()} *****")
            logger.app_success(_step)

            # Service data
            _step = 'Adding JCOMM and Sidecar Fields to Lead Generation Data'
            df_leads = self.pipeline_add_jcomm_sidecar(df_leads)
            logger.app_success(_step)

            # Post Process : Leads
            _step = 'Post Process output before formatting to calculate standard offering.'
            df_leads = self.post_proecess_leads(df_leads)
            logger.app_success(_step)

            _step = "Post Processing and Deriving columns on output iLeads"
            df_leads = self.post_process_output_ilead(df_leads)
            logger.app_success(_step)

            # Post Process : InstallBase
            _step = "Post Processing and Deriving columns on reference install leads"
            df_leads = self.post_process_ref_install(df_leads)

            logger.app_success(_step)

            address_cols = [
                "End_Customer_Address",
                "ShipTo_Street",
                "BillingAddress",
                "StartupAddress"
            ]
            for col in address_cols:
                df_leads[col] = df_leads[col].str.replace("\n", " ")
                df_leads[col] = df_leads[col].str.replace("\r", " ")

            _step = 'Write output lead to result directory'

            df_leads = df_leads.drop(
                columns=['temp_column', 'component', 'ClosedDate']) \
                .reset_index(drop=True)

            IO.write_csv(self.mode,
                         {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name':
                              self.config['file']['Processed']['output_iLead'][
                                  'validation']
                          }, df_leads)
            logger.app_success(_step)

            _step = "Formatting Output"

            ref_install_output_format = self.config['output_format'][
                'ref_install_base']
            ref_install = self.format.format_output(df_leads,
                                                    ref_install_output_format)

            ilead_output_format = self.config['output_format']['output_iLead']
            output_ilead = self.format.format_output(df_leads,
                                                     ilead_output_format)

            lead_type = output_ilead[["Serial_Number", "Lead_Type"]]
            lead_type["EOSL_reached"] = lead_type["Lead_Type"] == "EOSL"
            lead_type = lead_type.groupby("Serial_Number")["EOSL_reached"].any()
            ref_install = ref_install.merge(
                lead_type, on="Serial_Number", how="left"
            )

            _step = "Exporting reference install file"

            ref_install = ref_install.drop_duplicates(
                subset=['Serial_Number']). \
                reset_index(drop=True)

            IO.write_csv(self.mode,
                         {'file_dir': self.config['file']['dir_results'],
                          'file_name':
                              self.config['file']['Processed']['output_iLead'][
                                  'ref_install']
                          }, ref_install)

            logger.app_success(_step)

            _step = "Exporting output iLead file"

            IO.write_csv(self.mode,
                         {'file_dir': self.config['file']['dir_results'],
                          'file_name':
                              self.config['file']['Processed']['output_iLead'][
                                  'file_name']
                          }, output_ilead)

            logger.app_success(_step)

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return 'successfully !'

    #  ***** Pipelines ****
    def post_proecess_leads(self, df_leads):

        # All other displays: Invalid
        df_leads['is_standard_offering'] = False

        # PDU + ( M4 display  + 8212)
        ls_comp_type = ['M4 Display', '8212 Display']
        f_std_offer = (
                (df_leads.Product_M2M_Org.str.upper().isin(
                    ['PDU', 'PDU - PRIMARY', 'PDU - SECONDARY']))
                & df_leads.Component.isin(ls_comp_type)
                & df_leads.is_valid_logic_tray_lead
                & df_leads.is_valid_door_assembly_lead
                & df_leads.is_valid_input_breaker_panel_lead
        )

        df_leads.loc[f_std_offer, 'is_standard_offering'] = True
        # PDU + ( 'Monochrome Display', 'Color Display')
        ls_comp_type = ['Monochrome Display', 'Color Display']
        f_std_offer = (
                (df_leads.Product_M2M_Org.str.upper().isin(
                    ['PDU', 'PDU - PRIMARY', 'PDU - SECONDARY']))
                & df_leads.Component.isin(ls_comp_type)
                & df_leads.is_valid_door_assembly_lead
                & df_leads.is_valid_input_breaker_panel_lead
        )

        df_leads.loc[f_std_offer, 'is_standard_offering'] = True

        # RPP + ( M4 display  + 8212)
        ls_comp_type = ['Monochrome Display',
                        'Color Display', 'M4 Display', '8212 Display']
        f_std_offer = (
                (df_leads.Product_M2M_Org.str.upper() == 'RPP')
                & df_leads.Component.isin(ls_comp_type)
                & df_leads.is_valid_chasis_lead
                & (pd.to_datetime(df_leads.ShipmentDate) >= pd.to_datetime(
            "2008-01-01"))

        )

        df_leads.loc[f_std_offer, 'is_standard_offering'] = True
        # All other components: are valid
        ls_comp_type = ['BCMS', 'PCB', 'SPD', 'Fan', 'PDU', 'RPP', 'STS']
        f_std_offer = df_leads.Component.isin(ls_comp_type)
        df_leads.loc[f_std_offer, 'is_standard_offering'] = True

        return df_leads


    def post_process_ref_install(self, ref_install):
        """
        This module derives new columns for the blank columns after formatting the output.
        @param ref_install: ref_install output after formatting.
        @type ref_install: pd.Dataframe.
        """
        _step = "Deriving columns for output_iLead final data."
        try:

            _step = 'Deriving Product Age column using install date column'

            # TODO: for calculation if startup date has values used that else use shipment date.

            ref_install['Product_Age'] = (
                pd.Timestamp.now().normalize()
                - pd.to_datetime(ref_install['ShipmentDate'])) / np.timedelta64(1, 'Y')

            ref_install['Product_Age'] = ref_install['Product_Age'].astype(int)

            logger.app_success(_step)

            _step = 'Deriving is_under_contract column values based on contract end date & today'

            # Get the current date
            current_date = pd.Timestamp.now().normalize()

            # Create the 'is_under_contract' column based on conditions
            ref_install['is_under_contract'] = (
                                                       pd.to_datetime(
                                                           ref_install[
                                                               'Contract_Expiration_Date'],
                                                           errors='coerce') >= current_date
                                               ) & ~(
                ref_install['Contract_Expiration_Date'].isna())

            logger.app_success(_step)

            # TODO: These columns needs to be derived later as of now values a populated False
            # TODO: mapping in config is set to empty that should be assigned later to column
            # TODO: from where the column values will be formatted.

            ref_install.loc[:, 'flag_decommissioned'] = False
            ref_install.loc[:, 'flag_prior_lead'] = False
            ref_install.loc[:, 'flag_prior_service_lead'] = False

            # Area
            ref_area = IO.read_csv(
                self.mode, {
                    'file_dir': self.config['file']['dir_ref'],
                    'file_name': self.config['file']['Reference'][
                        'area_region']
                })

            ref_install['Key_region'] = ref_install['StartupState'].copy()
            ref_install.loc[
                pd.isna(ref_install['Key_region']), 'Key_region'] = \
            ref_install['ShipTo_State']

            ref_area.Abreviation = ref_area.Abreviation.str.lower()
            ref_install = ref_install.merge(
                ref_area[['Abreviation', "Region", 'CSE Area']],
                left_on='Key_region', right_on='Abreviation', how="left")
            del ref_install['Key_region']

            # *** BillTo customer ***
            # ref_install = self.get_billto_data(ref_install)

            # *** Strategic account ***
            ref_install = self.update_strategic_acoount(ref_install)

            return ref_install
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

    def update_strategic_acoount(self, ref_install):
        """

        :param ref_install: DESCRIPTION
        :type ref_install: TYPE
        :return: DESCRIPTION
        :rtype: pandas dataframe

        """
        # Update customer name
        ref_install['Customer_old'] = ref_install['Customer'].copy()
        ref_install['Customer'] = obj_filt.prioratized_columns(
            ref_install, ['StartupCustomer', 'ShipTo_Customer', 'BillingCustomer'])
        ref_install = ref_install.rename(columns={'StrategicCustomer': 'StrategicCustomer_old'})

        #ref_install = ref_install.rename({"Customer_old": "Customer", "Customer": "End_Customer"})

        # Update strategic account logic
        obj_sc = StrategicCustomer('local')
        df_customer = obj_sc.main_customer_list(df_leads=ref_install)
        df_customer = df_customer.drop_duplicates(subset=['Serial_Number'])

        ref_install = ref_install.merge(
            df_customer[['Serial_Number', 'StrategicCustomer']],
            left_on='SerialNumber_M2M', right_on="Serial_Number", how='left')

        # End To Custoimer details
        ref_install['EndCustomer'] = ref_install['Customer'].copy()
        dict_cols = {
            "End_Customer_Address": ['StartupAddress', 'ShipTo_Street'],
            "End_Customer_City": ['StartupCity', 'ShipTo_City'],
            "End_Customer_State": ['StartupState', 'ShipTo_State'],
            "End_Customer_Zip": ['StartupPostalCode', 'ShipTo_Zip']
            }
        for col in dict_cols:
            ls_cols = ['was_startedup'] + dict_cols[col]
            # Startup columns
            ref_install.loc[:, col] = ref_install[ls_cols].apply(
                lambda x: x[1] if x[0] else x[2], axis=1
            )
        return ref_install

    def post_process_output_ilead(self, output_ilead_df):
        """
        This module derives new columns for the blank columns after formatting the output.
        @param output_ilead_df: iLead output after formatting.
        @type output_ilead_df: pd.Dataframe.
        """
        _step = "Deriving columns for output_iLead final data."
        try:

            # Calculate the current year
            current_year = pd.Timestamp.now().year

            _step = "Derive column Component_Due_Date based on Lead_Type, Component_Date_Code"
            # Apply the custom function to create the 'Component_Due_Date' column
            output_ilead_df['Component_Due_Date'] = output_ilead_df.apply(
                self.calculate_component_due_date, axis=1)
            output_ilead_df['EOSL'] = output_ilead_df.apply(self.update_eosl,
                                                            axis=1)

            logger.app_success(_step)

            _step = 'Calculating Component_Due_in (years) using Component_Due_Date and current year'

            # Calculate the difference in years between 'Component_Due_Date' and the current year
            output_ilead_df['Component_Due_in (years)'] = pd.to_datetime(
                output_ilead_df['Component_Due_Date']).dt.year - current_year

            # Convert the 'year_difference' to integer
            output_ilead_df['Component_Due_in (years)'] = output_ilead_df[
                'Component_Due_in (years)'].astype(int)

            logger.app_success(_step)

            _step = 'Deriving Component_Due_in (Category) based on Due years'

            # Apply the function to create the 'Component_Due_in (Category)' column
            output_ilead_df['Component_Due_in (Category)'] = output_ilead_df[
                'Component_Due_in (years)'].apply(self.categorize_due_in_category)

            # Add prod meta data
            output_ilead_df = self.prod_meta_data(output_ilead_df)

            return output_ilead_df
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e


    def prod_meta_data(self, output_ilead_df):
        # Partnumber for chasis decides the axle
        _step = "Product meta data"
        try:
            # Read reference data
            ref_chasis = IO.read_csv(
                self.mode, {
                    'file_dir': self.config['file']['dir_ref'],
                    'file_name': self.config['file']['Reference']['chasis']
                    })
            ref_chasis = ref_chasis.drop_duplicates(subset=['key_chasis'])

            # Get part numbers
            output_ilead_df['key_chasis'] = output_ilead_df['pn_chasis'].copy()
            output_ilead_df['key_chasis']= output_ilead_df['key_chasis'].fillna("(")
            output_ilead_df.loc[:, 'key_chasis'] = output_ilead_df[
                'key_chasis'].apply(
                    lambda x: re.split(", | \(", x)[0] if x[0] != "(" else "")

            # Attach data
            output_ilead_df = output_ilead_df.merge(
                ref_chasis, on = 'key_chasis', how='left')

            return output_ilead_df
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e




    def pipeline_add_jcomm_sidecar(self, df_leads: object, service_df=None):
        """
        This module read's services data and joins with the lead data and add jcomm, sidecar fields.
        @param service_df: intermediate generated services data.
        @type df_leads: object : leads data after identifying leads.
        """
        _step = 'Read Services data and append has_jcomm, has_sidecar field to lead data'
        try:
            if service_df is not None:
                df_service_jcomm_sidecar = service_df
            else:
                df_service_jcomm_sidecar = IO.read_csv(self.mode,
                                                       {'file_dir':
                                                            self.config[
                                                                'file'][
                                                                'dir_results'] +
                                                            self.config[
                                                                'file'][
                                                                'dir_intermediate'],
                                                        'file_name':
                                                            self.config[
                                                                'file'][
                                                                'Processed']
                                                            ['services'][
                                                                'intermediate']
                                                        })

            df_service_jcomm_sidecar = df_service_jcomm_sidecar[
                ['SerialNumber', 'Has_JCOMM',
                 'Has_Sidecar']]

            df_service_jcomm_sidecar.rename(
                columns={'SerialNumber': 'SerialNumber_M2M'},
                inplace=True)

            df_leads = df_leads.merge(df_service_jcomm_sidecar,
                                      left_on='SerialNumber_M2M',
                                      right_on='SerialNumber_M2M', how='left')

            logger.app_success(_step)
            return df_leads
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

    def pipeline_bom_identify_lead(self, df_install):  # pragma: no cover
        """
        This method reads the bom data joins it with install data and then uses the input
        reference lead file to generate the leads.
        @param df_install: processed install base data coming from contract pipeline
        @return: return a df_lead after identifying the leads.
        """
        # Read : Reference lead opportunities
        _step = "Read : Reference lead opportunities"
        try:
            ref_lead_opp = IO.read_csv(
                self.mode, {
                    'file_dir': self.config['file']['dir_ref'],
                    'file_name': self.config['file']['Reference'][
                        'lead_opportunities']
                })

            # Read : Raw BOM data
            _step = "Read raw data : BOM"

            df_bom = IO.read_csv(
                self.mode, {
                    'file_dir': self.config['file']['dir_data'],
                    'file_name': self.config['file']['Raw']['bom'][
                        'file_name'],
                    'sep': '\t'})

            input_format = self.config['database']['bom']['Dictionary Format']
            df_bom = self.format.format_data(df_bom, input_format)

            # Merge raw bom data with processed_merge_contract_install dataframe
            _step = 'Merge data: Install and BOM'

            df_bom = self.pipeline_merge(df_bom, df_install, type_='lead_id')
            logger.app_success(_step)

            ref_lead_opp = ref_lead_opp.dropna(
                subset=['EOSL', 'Life__Years'], how='all') \
                .reset_index(drop=True)

            # Identify Lead from Part Number TLN and BOM

            _step = 'Identify Lead for BOM'
            df_leads = self.identify_leads(df_bom, ref_lead_opp)

            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_leads

    def update_sts_leads(self, df_leads):
        """
        Update component life for STS
        :param df_leads: Generated leads
        :type df_leads: pandas DataFrame
        :raises Exception: captures all
        :return: DESCRIPTION
        :rtype: pandas data frame
        """

        _step = 'Update component life for STS'
        try:
            df_leads['flag_update_sts_leads'] = (
                        (df_leads['Product_M2M_Org'].str.lower() == 'sts') &
                        (df_leads['Component'].str.lower().isin(
                            ['spd', 'pcb', 'color display',
                             'monochrome display', 'm4 display',
                             '8212 display'])))
            df_leads.loc[df_leads.flag_update_sts_leads, 'Life__Years'] = 7
            return df_leads
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

    def pipeline_merge_lead_services(self, df_leads, df_services=None):
        """
        This functions reads the services intermediate data and join with the leads data to
        generate a date code column.
        @param df_services: this is adding for testing purpose (processed services data)
        @param df_leads: intermediate leads dataframe.
        @return: return a df_lead merging services with leads dataframe.
        """
        _step = "Merging leads and services data to extract date code at component level"
        try:
            if df_services is None:
                df_services = IO.read_csv(self.mode,
                                          {'file_dir': self.config['file'][
                                                           'dir_results'] +
                                                       self.config['file'][
                                                           'dir_intermediate'],
                                           'file_name':
                                               self.config['file']['Processed']
                                               ['services']['file_name']
                                           })

            # Convert to correct date format
            df_services['ClosedDate'] = pd.to_datetime(
                df_services['ClosedDate'], errors='coerce'). \
                dt.strftime('%Y-%m-%d')

            df_services.sort_values(by='ClosedDate', ascending=False,
                                    inplace=True)

            # Identify the duplicate rows based on the two columns and keep the last occurrence
            # using this because when there is no duplicate drop_duplicate can result none
            # when we use keep last.
            duplicates_mask = df_services.duplicated(
                subset=['SerialNumber', 'component'],
                keep='last')

            # Invert the mask to keep the non-duplicate rows
            df_services = df_services[~duplicates_mask]

            unique_component = []
            for i in list(df_services['component'].unique()):
                if "fan" in i.lower():
                    unique_component.append(i.replace("s", "").strip().lower())
                else:
                    unique_component.append(i.strip().lower())

            df_services = df_services[
                ['SerialNumber', 'component', 'ClosedDate']]

            df_services['component'] = df_services['component'].str.lower()

            # Convert to correct date format
            df_leads['InstallDate'] = pd.to_datetime(df_leads['InstallDate'],
                                                     errors='coerce'). \
                dt.strftime('%Y-%m-%d')

            df_leads['Component'] = df_leads['Component'].fillna("")
            # Create a new column based on the list values
            df_leads['temp_column'] = df_leads['Component'].apply(
                lambda x: next(
                    (val for val in unique_component if val in x.lower()), x))

            df_services.rename(columns={'SerialNumber': 'SerialNumber_M2M'},
                               inplace=True)

            df_leads = df_leads.merge(df_services, left_on=['SerialNumber_M2M',
                                                            'temp_column'],
                                      right_on=['SerialNumber_M2M',
                                                'component'], how='left')

            # Replace NaN with empty string
            df_leads = df_leads.fillna('')
            df_leads = df_leads.reset_index(drop=True)

            # Use np.where to create the new column
            df_leads['date_code'] = np.where(df_leads['ClosedDate'] != '',
                                             df_leads['ClosedDate'],
                                             df_leads['InstallDate'])

            # Use np.where to create the new column
            df_leads['source'] = np.where(df_leads['ClosedDate'] != '',
                                          'Services', 'InstallBase')

            df_leads = df_leads.drop_duplicates().reset_index(drop=True)

            logger.app_success(_step)
            return df_leads
        except Exception as excp:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from excp

    def pipeline_merge(self, df_bom, df_install, type_):
        """
        This method merges the bom data and processed install base data on two conditions.
        Either we want to join for purpose of lead generation or for purpose of adding metadata.
        @param df_bom: this is raw bom data.
        @param df_install: A pandas dataframe processed from contracts pipeline.
        @param type_: pd.Dataframe
        @return: pd.Dataframe
        """
        _step = f'Query install data ({type_}'
        try:
            ls_cols = ['Job_Index', 'InstallDate', 'Product_M2M',
                       'SerialNumber_M2M', 'Revision', 'PartNumber_TLN_Shipment']

            if type_ == 'lead_id':
                key = 'Job_Index'

                if 'InstallDate' not in df_install.columns:
                    df_install['InstallDate'] = df_install['startup_date']. \
                        fillna(df_install['ShipmentDate'])
                else:
                    df_install['InstallDate'] = df_install['startup_date']. \
                        fillna(df_install['InstallDate'])

                # Product_M2M : change request by stephen on 17 July, 23.
                df_install['Product_M2M'] = df_install[
                    'product_prodclass'].str.lower()

                # Divide the install base data into made to order(mto) and made
                # to stock(mts) category. The entries with job_index are made
                # to batch, entries without job_index is made to stock
                df_install_mto = df_install.loc[
                    df_install["Job_Index"].notna()
                ]
                df_install_mts = df_install.loc[
                    df_install["Job_Index"].isna()
                ]

                # Changed join from "inner" to "right" on 19th July 23 (Bug CIPILEADS-533)
                # Joined  made to batch data with bom data on 23rd Oct, 23
                df_out_mto = df_bom.merge(df_install_mto[ls_cols], on=key, how='right')

                # Added component part numbers for made to stock data on 23rd Oct, 23
                df_out_mts = self.add_data_mts(df_install_mts[ls_cols], merge_type='left')

                # Append the custom data, batch empty data and batch non_empty data
                df_out = df_out_mto._append(
                    df_out_mts, ignore_index=True
                )

            elif type_ == 'meta_data':
                key = 'SerialNumber_M2M'
                ls_cols = [col for col in df_install.columns if
                           col not in ls_cols]
                ls_cols = ls_cols + ['SerialNumber_M2M']
                ls_cols.remove('SerialNumber')
                # Changed join from "inner" to "right" on 19th July 23 (Bug CIPILEADS-533)
                df_out = df_bom.merge(df_install[ls_cols], on=key, how='right')

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e
        return df_out

    def pipeline_contract_install(self):  # pragma: no cover
        """
        This returns the processed install base data from contracts pipeline.
        @return: pd.Dataframe
        """
        # Read : Contract Processed data
        _step = "Read processed contract data"
        try:
            df_contract = IO.read_csv(self.mode, {
                'file_dir': self.config['file']['dir_results'] +
                            self.config['file'][
                                'dir_intermediate'],
                'file_name': self.config['file']['Processed'][
                    'contracts']['file_name']})

            df_contract = df_contract.drop_duplicates(
                subset=['SerialNumber_M2M']) \
                .reset_index(drop=True)

            df_contract = df_contract.dropna(subset=['Product_M2M'])

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_contract

    # ***** Lead Identification *****

    def identify_leads(self, df_bom, ref_lead_opp):  # pragma: no cover
        """
        These methods identify leads based on ProductM2M and PartNumber_BOM_BOM
        @param df_bom: Raw Bom data.
        @param ref_lead_opp: reference leads data on which leads are generated.
        @return: pd.Dataframe
        """
        df_bom['Product_M2M_Org'] = df_bom['Product_M2M'].copy()
        ls_cols_ref = [
            '',
            'Match', 'Component', 'Component_Description', 'End of Prod',
            'Status',
            'Life__Years', 'EOSL', 'flag_raise_in_gp']
        ls_cols = ['Job_Index', 'Total_Quantity', '', 'InstallDate',
                   'Product_M2M_Org',
                   'SerialNumber_M2M', 'key_value']

        # Identify Leads: TLN
        _step = "Identify Leads: TLN"
        try:
            # Lead_id_bases is changed from PartNumber_TLN_BOM
            lead_id_basedon = 'Product_M2M'

            # Subset data : Ref
            ls_cols_ref[0] = lead_id_basedon

            # filter reference lead dataframe where value in Product_M2M is empty or na
            ref_lead = ref_lead_opp.loc[
                pd.notna(ref_lead_opp[ls_cols_ref[0]]), ls_cols_ref]

            # filter rows from reference lead dataframe where Product_M2M value is "blank"
            ref_lead = ref_lead[~ref_lead[lead_id_basedon].isin(['blank', ''])]

            # Subset data : BOM
            ls_cols[2] = lead_id_basedon

            # dropping duplicates on reference lead and bom_install dataframe
            # df_bom = df_bom[ls_cols[:-1]].drop_duplicates()
            ref_lead = ref_lead.drop_duplicates(
                subset=[lead_id_basedon, 'Component']) \
                .reset_index(drop=True)
            df_data = df_bom.drop_duplicates(
                subset=[lead_id_basedon, 'SerialNumber_M2M']) \
                .reset_index(drop=True)
            df_data['key_value'] = df_data[lead_id_basedon]

            # Id Leads
            df_leads_tln = self.id_leads_for_partno(
                df_data, ref_lead, lead_id_basedon)
            del df_data, ref_lead
            logger.app_info(
                "Leads After PartNumber_M2M is :" + str(df_leads_tln.shape))

            # Identify Leads: BOM
            _step = "Identify Leads: BOM"
            lead_id_basedon = 'PartNumber_BOM_BOM'

            # Subset data : Ref
            ls_cols_ref[0] = lead_id_basedon
            ref_lead = ref_lead_opp.loc[
                pd.notna(ref_lead_opp[ls_cols_ref[0]]), ls_cols_ref]

            # Subset data : BOM
            ls_cols[2] = lead_id_basedon
            df_data = df_bom[ls_cols[:-1]].drop_duplicates()
            df_data['key_value'] = df_data[lead_id_basedon]

            df_data["PartNumber_BOM_BOM"] = df_data["PartNumber_BOM_BOM"].fillna("")
            # Id Leads
            df_leads_bom = self.id_leads_for_partno(
                df_data, ref_lead, lead_id_basedon)

            del df_data, ref_lead
            logger.app_info("Leads After PartNumber_BOM_BOM is :" + str(
                df_leads_bom.shape))

            # Merge leads
            _step = "Merge Leads from TLN and BOM"

            df_leads_out = pd.concat([df_leads_tln, df_leads_bom])
            del df_leads_tln, df_leads_bom

            _step = "Update lead for STS"

            df_leads_out = self.update_sts_leads(df_leads_out)

            IO.write_csv(self.mode,
                         {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file']['dir_validation'],
                          'file_name':
                              self.config['file']['Processed']['output_iLead'][
                                  'before_classify']
                          }, df_leads_out)

            logger.app_debug(
                f'No of leads b4 classify: {df_leads_out.shape[0]}')

            # Update 'PCB08212' component type based on shipment date, 1 Nov, 2023
            part_nums = ['pcb08212', 'sa00012', 'sa00013']
            for part_num in part_nums:
                df_leads_out.loc[
                    (
                            (df_leads_out.key == part_num) &
                            (df_leads_out.InstallDate.astype(
                                str) < '2015-01-01')
                    ),
                    'Component'
                ] = 'Monochrome Display'
            part_nums = ['sa00012', 'sa00013']
            for part_num in part_nums:
                df_leads_out.loc[
                    (
                            (df_leads_out.key == part_num) &
                            (df_leads_out.InstallDate.astype(
                                str) >= '2015-01-01')
                    ),
                    ['Component', 'Status', 'Life__Years', 'EOSL']
                ] = ['Color Display', 'Active', 10, np.nan]


            df_leads_out = self.classify_lead(df_leads_out)
            logger.app_debug(
                f'No of leads after classify: {df_leads_out.shape[0]}')

            IO.write_csv(self.mode,
                         {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name':
                              self.config['file']['Processed']['output_iLead'][
                                  'after_classify']
                          }, df_leads_out)
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_leads_out

    def id_leads_for_partno(self, df_data, ref_lead, lead_id_basedon):

        """

        Reference file identifies the leads based on TLN i.e. assembly level parts. Only one
        pattern exists i.e. bigns_with. In future  if new patterns are identified,
        code would require changes.
        @param df_data: This the processed bom and install data
        @type df_data: pd.Dataframe
        @param ref_lead: this input reference lead data.
        @type ref_lead: pd.Dataframe
        @param lead_id_basedon: this is a column based on which leads is generated.
        @type lead_id_basedon: str

        """
        _step = f'Identify lead based on {lead_id_basedon}'
        try:
            # Input
            types_patterns = ref_lead.Match.unique()

            # Initialize Output
            df_out = pd.DataFrame()

            # total 12 columnns as output
            ls_col_out = [
                'Job_Index', 'Total_Quantity',
                'Component', 'Component_Description',
                'key', 'End of Prod', 'Status', 'Life__Years',
                'EOSL', 'flag_raise_in_gp', 'SerialNumber_M2M',
                'Product_M2M_Org']

            # Prep reference file
            # rename 'Product_M2M' or "PartNumber_BOM_BOM" column to "key"
            ref_lead = ref_lead.rename(
                columns={ref_lead.columns[0]: 'key'})

            # find the length on values in key column and store them in a new col called "len_key"
            ref_lead['len_key'] = ref_lead.key.str.len()

            # sort the values in descending order i.e 3,2,1 based on values of "len_key" column.
            ref_lead = ref_lead.sort_values(by=['len_key'], ascending=False)

            # convert all the values in "key" column to lowercase
            ref_lead['key'] = ref_lead['key'].str.lower()

            # Add two more column names to output list of columns after adding total 14 columns
            ls_col_out += ['InstallDate', 'key_value']

            # Identify leads where PartNumber matches exactly.
            # if lead is identified for PartNumber;
            # then it will be dropped from further processing

            if 'exact' in types_patterns:
                # filter reference lead data based on "match" columns values
                df_ref_sub = ref_lead[ref_lead.Match == 'exact']
                df_cur_out, df_data = self.lead4_exact_match(
                    df_data, df_ref_sub, lead_id_basedon, ls_col_out)

                # if the current output has more than 0 rows then add a new column "lead_match"
                # assign the values as "exact" in this column
                if df_cur_out.shape[0] > 0:
                    df_cur_out.loc[:, 'lead_match'] = 'exact'

                    df_out = pd.concat([df_out, df_cur_out])
                del df_ref_sub, df_cur_out

            if 'contains' in types_patterns:
                # filter reference lead data based on "match" columns values
                df_ref_sub = ref_lead[ref_lead.Match == 'contains']
                df_cur_out, df_data = self.lead4_contains(
                    df_data, df_ref_sub, lead_id_basedon, ls_col_out)

                # if the current output has more than 0 rows then add a new column "lead_match"
                # assign the values as "contains" in this column
                if df_cur_out.shape[0] > 0:
                    df_cur_out.loc[:, 'lead_match'] = 'contains'
                    df_out = pd.concat([df_out, df_cur_out])
                del df_ref_sub, df_cur_out

            if 'begins_with' in types_patterns:
                # filter reference lead data based on "match" columns values
                df_ref_sub = ref_lead[ref_lead.Match == 'begins_with']
                df_cur_out, df_data = self.lead4_begins_with(
                    df_data, df_ref_sub, lead_id_basedon, ls_col_out)

                # if the current output has more than 0 rows then add a new column "lead_match"
                # assign the values as "begins_with" in this column
                if df_cur_out.shape[0] > 0:
                    df_cur_out.loc[:, 'lead_match'] = 'begins_with'
                    df_out = pd.concat([df_out, df_cur_out])
                del df_ref_sub, df_cur_out

            # Metadata,
            # add a column called as "lead_id_basedon" which indicate the lead was calculated
            # on what type of column either PartNumber_TLN_BOM or PartNumber_BOM_BOM
            df_out['lead_id_basedon'] = lead_id_basedon
            logger.app_debug(f'{_step} : SUCCEEDED', 1)

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e
        return df_out

    # ***** Match ref data for lead identification *****
    def lead4_exact_match(self, df_temp_data, df_ref_sub, lead_id_basedon,
                          ls_col_out):
        """
        This method runs when there is an exact keyword in Match column in reference leads
        @param df_temp_data: processed bom and install data
        @param df_ref_sub: reference lead data after filtering
        @param lead_id_basedon: column based on which leads will be generated
        @param ls_col_out: list of output columns
        @return: pd.Dataframe.
        """
        _step = f'identify leads key: {lead_id_basedon}, match: exact'

        try:
            logger.app_info(f'{_step}: STARTED')
            org_size = df_temp_data.shape[0]
            ls_col_in = df_temp_data.columns
            logger.app_debug(
                f'Data Size Original : {df_temp_data.shape[0]}; ')

            df_temp_data['key'] = df_temp_data[lead_id_basedon].copy()
            df_temp_data = df_temp_data.drop_duplicates()
            df_temp_data['key'] = df_temp_data['key'].str.lower()

            df_ref_sub = df_ref_sub.drop_duplicates(
                subset=['key', 'Component'])

            df_decoded = df_temp_data.merge(df_ref_sub, on='key', how='left')

            # Consolidated leads. For TLN where lead has been identified,
            # will be added to output

            if any(pd.notna(df_decoded.Component)):
                df_out_sub = df_decoded.loc[
                    pd.notna(df_decoded.Component), ls_col_out]
            else:
                df_out_sub = pd.DataFrame()

            # df_temp_data: Remove keys with identified lead from further processing

            df_temp_data = df_decoded.loc[
                pd.isna(df_decoded.Component), ls_col_in]

            # Cross-checking
            new_size = df_temp_data.shape[0]

            del df_ref_sub, df_decoded

            logger.app_debug(
                f'New Size: {new_size}; '
                f'Size Drop : {org_size - new_size}')

            logger.app_debug(f'{_step} : SUCCEEDED', 1)

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e
        return df_out_sub, df_temp_data

    def lead4_begins_with(self, df_temp_data, df_ref_sub, lead_id_basedon,
                          ls_col_out):
        """
        This method runs when there is a begin_with keyword in Match column in reference leads
        @param df_temp_data: processed bom and install data
        @param df_ref_sub: reference lead data after filtering
        @param lead_id_basedon: column based on which leads will be generated
        @param ls_col_out: list of output columns
        @return: pd.Dataframe.
        """

        _step = f'identify leads key: {lead_id_basedon}, match: being_with'
        try:
            logger.app_info(f'{_step}: STARTED')
            org_size = df_temp_data.shape[0]
            df_ref_sub = df_ref_sub.drop_duplicates(
                subset=['key', 'Component'])

            df_out_sub = pd.DataFrame()
            ls_key_len = df_ref_sub.len_key.unique()
            ls_col_in = df_temp_data.columns

            logger.app_debug(
                f'Data Size Original : {df_temp_data.shape[0]}; ')

            for key_len in ls_key_len:
                # key_len = ls_key_len[0]
                org_size = df_temp_data.shape[0]

                df_temp_data['key'] = df_temp_data[lead_id_basedon].apply(
                    lambda x: x[:key_len].lower())

                df_decoded = df_temp_data.merge(df_ref_sub, on='key',
                                                how='left')

                # Consolidated leads. For TLN where lead has been identified,
                # will be added to output

                if any(pd.notna(df_decoded.Component)):
                    df_cur_out = df_decoded.loc[
                        pd.notna(df_decoded.Component), ls_col_out]
                else:
                    df_cur_out = pd.DataFrame()
                df_out_sub = pd.concat([df_out_sub, df_cur_out])

                # Filter data from further processing for keys with lead identified

                df_temp_data = df_decoded.loc[
                    pd.isna(df_decoded.Component), ls_col_in]

                # Cross-checking
                new_size = df_temp_data.shape[0]

                logger.app_debug(
                    f'For keylen: {key_len}; '
                    f'New Size: {new_size}; '
                    f'Size Drop : {org_size - new_size}')

                del df_cur_out, df_decoded

            logger.app_debug(f'{_step} : SUCCEEDED', 1)

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_out_sub, df_temp_data

    def lead4_contains(self, df_temp_data, df_ref_sub, lead_id_basedon,
                       ls_col_out):
        """
        This method runs when there is a contains keyword in Match column in reference leads
        @param df_temp_data: processed bom and install data
        @param df_ref_sub: reference lead data after filtering
        @param lead_id_basedon: column based on which leads will be generated
        @param ls_col_out: list of output columns
        @return: pd.Dataframe.
        """
        _step = f'identify leads key: {lead_id_basedon}, match: contains'
        try:
            logger.app_info(f'{_step}: STARTED')
            org_size = df_temp_data.shape[0]
            df_ref_sub = df_ref_sub.drop_duplicates(
                subset=['key', 'Component'])

            df_out_sub = pd.DataFrame()
            ls_key = df_ref_sub['key'].unique()
            ls_col_in = df_temp_data.columns

            logger.app_debug(
                f'Data Size Original : {df_temp_data.shape[0]}; ')

            for key in ls_key:
                # key = ls_key[0]
                org_size = df_temp_data.shape[0]

                df_temp_data['flag_valid'] = df_temp_data[
                    lead_id_basedon].str.contains(key, case=False)

                # Consolidated leads. For TLN where lead has been identified,
                # will be added to output

                if any(pd.notna(df_temp_data.flag_valid)):
                    df_ref_lukup = df_ref_sub[df_ref_sub.key == key]

                    df_cur_out = df_temp_data[
                        df_temp_data['flag_valid']].copy()
                    df_temp_data['key'] = df_temp_data[lead_id_basedon]
                    df_cur_out['left_key'] = key

                    df_cur_out = df_cur_out.merge(
                        df_ref_lukup, how='left',
                        left_on='left_key', right_on='key')
                else:
                    df_cur_out = pd.DataFrame()
                df_out_sub = pd.concat([df_out_sub, df_cur_out[ls_col_out]])

                # Filter data from further processing for keys with lead identified
                df_temp_data = df_temp_data.loc[
                    df_temp_data.flag_valid == False, ls_col_in]

                # Cross-checking
                new_size = df_temp_data.shape[0]

                logger.app_debug(
                    f'New Size: {new_size}; '
                    f'Size Drop : {org_size - new_size}')

                del df_cur_out

            logger.app_debug(f'{_step} : SUCCEEDED', 1)

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_out_sub, df_temp_data

    # ***** Classify leads *****
    def classify_lead(self, df_leads_wn_class, test_services=None):
        """
        There are 2 types of component replacement leads for DCPD:
            1. EOSL: if a component reaches its End of Service Life
            2. Life: based on age of component if it has reached its design life
                if Life of component > design life, then only lead will be raised.

        :param df_leads_wn_class: BOM data mapped with lead opportunities
        :type df_leads_wn_class: pandas dataframe
        :raises Exception: DESCRIPTION
        :return: leads classified as EOSL or Life.
        :rtype: pandas dataframe
        :param test_services: this is added for unit testing purpose.

        """
        _step = 'Lead classification'
        try:
            df_leads = pd.DataFrame()

            # Merge generated leads and services data
            try:
                _step = 'Merge lead and services data'
                if test_services is None:
                    df_leads_wn_class = self.pipeline_merge_lead_services(
                        df_leads_wn_class)
                logger.app_success(_step)
            except Exception as e:
                logger.app_fail(_step, f"{traceback.print_exc()}")
                raise Exception from e

            # Derive fields
            df_leads_wn_class.loc[:, 'date_code'] = pd.to_datetime(
                df_leads_wn_class['date_code'])
            df_leads_wn_class.loc[:, 'today'] = pd.to_datetime(
                pd.Timestamp.now())

            df_leads_wn_class['age'] = (
                    (df_leads_wn_class['today'] - df_leads_wn_class[
                        'date_code'])
                    / np.timedelta64(1, 'Y')).round().astype(int)

            # EOSL Leads
            df_leads_wn_class.EOSL = df_leads_wn_class.EOSL.fillna("")
            flag_lead_eosl = df_leads_wn_class.EOSL != ''

            if any(flag_lead_eosl):
                df_leads_sub = df_leads_wn_class[flag_lead_eosl].copy()
                df_leads_sub.loc[:, 'lead_type'] = 'EOSL'
                df_leads = pd.concat([df_leads, df_leads_sub])
                del df_leads_sub
            del flag_lead_eosl

            df_leads_wn_class = df_leads_wn_class[
                df_leads_wn_class['EOSL'] == '']
            # For DCPD leads due this year will be processed
            df_leads_wn_class.Life__Years = pd.to_numeric(
                df_leads_wn_class.Life__Years)
            flag_lead_life = pd.notna(df_leads_wn_class.Life__Years)
            if any(flag_lead_life):
                df_leads_sub = df_leads_wn_class[flag_lead_life].copy()
                df_leads_sub['Life__Years'] = df_leads_sub[
                    'Life__Years'].replace('', pd.NA)
                df_leads_sub['Life__Years'] = pd.to_numeric(
                    df_leads_sub['Life__Years'],
                    errors='coerce').fillna(0).astype(int)
                df_leads_sub.loc[:, 'flag_include'] = (
                        df_leads_sub['age'] > df_leads_sub['Life__Years'])

                # if any(df_leads_sub.flag_include):
                df_leads_sub.loc[:, 'lead_type'] = 'Life'
                # df_leads_sub = df_leads_sub[df_leads_sub['flag_include']]
                df_leads = pd.concat([df_leads, df_leads_sub])

                del flag_lead_life

            logger.app_debug(f'{_step} : SUCCEEDED', 1)

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_leads

    def calculate_component_due_date(self, row):
        """
        Define a custom function to calculate the 'Component_Due_Date'
        """
        if row['lead_type'] == 'EOSL':
            eosl_year = int(row['EOSL'])
            first_day_of_year = pd.to_datetime(f'01/01/{eosl_year}',
                                               format='%m/%d/%Y')
            return first_day_of_year.strftime('%m/%d/%Y')

        component_date_code = pd.to_datetime(row['date_code'],
                                             format='%m/%d/%Y')
        component_life_years = pd.DateOffset(years=row['Life__Years'])
        return (component_date_code + component_life_years).strftime(
            '%m/%d/%Y')

    def update_eosl(self, row):
        """
        Changes the format of EOSL column from year to date
        """
        if row['lead_type'] == 'EOSL':
            return row['Component_Due_Date']
        return row['EOSL']

    def add_data_mts(self, df_install_mts, merge_type):
        """
        Method to join made to stock data with standard bom data
        :param df_install_mts: Made to stock install data
        :param merge_type: merge type
        :return: Merged data
        """
        # Read Standard BOM data and preprocess standard bom data
        df_bom_data_default = IO.read_csv(
            self.mode,
            {'file_dir': self.config['file']['dir_data'],
             'file_name': self.config['file']['Raw']['bomdata_deafault']['file_name']}
        )
        df_bom_sisc = IO.read_csv(
            self.mode,
            {'file_dir': self.config['file']['dir_data'],
             'file_name': self.config['file']['Raw']['bomdata_sisc']['file_name']}
        )
        df_standard_bom = df_bom_sisc._append(
            df_bom_data_default, ignore_index=True
        )
        df_standard_bom.rename(
            columns={
                "fparent": "PartNumber_TLN_Shipment",
                "fcomponent": "PartNumber_BOM_BOM",
                "fparentrev": "Revision",
                "fqty": "Total_Quantity"
            }, inplace=True
        )
        df_standard_bom["PartNumber_TLN_Shipment"] = df_standard_bom[
            "PartNumber_TLN_Shipment"].str.lstrip().str.rstrip()
        df_standard_bom["PartNumber_BOM_BOM"] = df_standard_bom[
            "PartNumber_BOM_BOM"].str.lstrip().str.rstrip()
        df_standard_bom = df_standard_bom[
            ['PartNumber_TLN_Shipment', 'PartNumber_BOM_BOM', 'Revision', "Total_Quantity"]
        ]

        # Preprocess made to stock data
        df_install_mts["PartNumber_TLN_Shipment"] = df_install_mts[
            "PartNumber_TLN_Shipment"].astype(str).str.lstrip().str.rstrip()

        # Merge made to stock data with standard bom data based on shipment
        # number and revision
        df_install_mts = df_install_mts.merge(
            df_standard_bom,
            on=['PartNumber_TLN_Shipment', 'Revision'],
            how=merge_type
        )

        # Further divide the mts data based on if the join was successful i.e.
        # obtained BOM number is null or not.
        # For the batch data where join fails, it doesn't seem to have a proper
        # revision, hence we merge it with the first revision available
        df_install_mts_joined = df_install_mts.loc[
            df_install_mts["PartNumber_BOM_BOM"].notna()
        ]
        df_install_mts_not_joined = df_install_mts.loc[
            df_install_mts["PartNumber_BOM_BOM"].isna()
        ]

        # Preprocess standard bom data to select the first revision for every shipment
        df_standard_bom[["PartNumber_TLN_Shipment", "Revision"]].fillna("", inplace=True)
        df_standard_bom['key'] = (
                df_standard_bom["PartNumber_TLN_Shipment"] + ":" +
                df_standard_bom["Revision"]
        )
        df_standard_bom.drop_duplicates(["PartNumber_TLN_Shipment", "key"],
                                        inplace=True)
        df_standard_bom.sort_values(by=["key"])
        df_standard_bom.drop_duplicates(["PartNumber_TLN_Shipment"],
                                        keep='first', inplace=True)
        df_standard_bom.drop(["key"], axis=1, inplace=True)

        # Merge the not joined data with the first revision available
        df_install_mts_not_joined.drop(
            ["PartNumber_BOM_BOM", "Revision", "Total_Quantity"],
            axis=1,
            inplace=True
        )
        df_install_mts_not_joined = df_install_mts_not_joined.merge(
            df_standard_bom,
            on=['PartNumber_TLN_Shipment'],
            how=merge_type
        )

        # Append joined data and unjoined data to get complete made to stock data
        df_install_mts = df_install_mts_joined._append(
            df_install_mts_not_joined, ignore_index=True
        )

        return df_install_mts

    def categorize_due_in_category(self, component_due_in_years):
        """
        Method categorises component due date
        :param component_due_in_years: Due date in years
        :return: Due category
        """
        if component_due_in_years < 0:
            return "Past Due"
        if 0 <= component_due_in_years <= 1:
            return "Due this year"
        if 1 < component_due_in_years <= 3:
            return "Due in 2-3 years"
        if 3 < component_due_in_years < 100:
            return "Due after 3 years"
        return "Unknown"  # Or any other default category you want to assign

#%%
if __name__ == "__main__":
    obj = LeadGeneration()
    obj.main_lead_generation()

# %%
