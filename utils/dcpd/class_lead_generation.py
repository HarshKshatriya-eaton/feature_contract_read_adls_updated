"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

import numpy as np
import pandas as pd
import traceback
from string import punctuation
from utils.dcpd.class_business_logic import BusinessLogic
from utils.dcpd.class_serial_number import SerialNumber
from utils.format_data import Format
from utils import AppLogger
from utils import IO

logger = AppLogger(__name__)
punctuation = punctuation + ' '


class LeadGeneration:
    def __init__(self, mode='local'):
        self.mode = mode
        self.srnum = SerialNumber()
        self.bus_logic = BusinessLogic()
        self.config = IO.read_json(mode='local', config={
            "file_dir": './references/', "file_name": 'config_dcpd.json'})
        self.format = Format()

    def main_lead_generation(self):  # pragma: no cover

        _step = 'Read Merged Contracts and Install Base data'
        try:
            df_install = self.pipeline_contract_install()
            logger.app_success(_step)
        except Exception as excp:
            logger.app_fail(_step, f'{traceback.format_exc()}')
            raise Exception('f"{_step}: Failed') from excp

        # ***** PreProcess BOM data *****
        _step = 'Process BOM data'
        try:
            # Read Data
            df_leads = self.pipeline_bom(df_install)
            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        _step = 'Merge data: Install and BOM'
        try:
            df_leads = self.pipeline_merge(df_leads, df_install, 'meta_data')
            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        _step = 'Adding JCOMM and Sidecar Fields to Lead Generation Data'
        try:
            df_leads = self.pipeline_add_jcomm_sidecar(df_leads)
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        _step = 'Write output lead to result directory'
        try:
            df_leads = df_leads.drop(columns=['temp_column', 'component', 'ClosedDate']) \
                .reset_index(drop=True)
            IO.write_csv(self.mode, {'file_dir': self.config['file']['dir_results'] +
                                                 self.config['file']['dir_validation'],
                                     'file_name': self.config['file']['Processed']['output_iLead'][
                                         'validation']
                                     }, df_leads)
            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        _step = "Formatting Output"
        try:

            ref_install_output_format = self.config['output_format']['ref_install_base']
            ref_install = self.format.format_output(df_leads, ref_install_output_format)

            IO.write_csv(self.mode, {'file_dir': self.config['file']['dir_results'],
                                     'file_name': self.config['file']['Processed']['output_iLead'][
                                         'ref_install']
                                     }, ref_install)

            iLead_output_format = self.config['output_format']['output_iLead']
            output_iLead = self.format.format_output(df_leads, iLead_output_format)

            IO.write_csv(self.mode, {'file_dir': self.config['file']['dir_results'],
                                     'file_name': self.config['file']['Processed']['output_iLead'][
                                         'file_name']
                                     }, output_iLead)

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return 'successfull !'

    # %% ***** Pipelines *****

    def pipeline_add_jcomm_sidecar(self, df_leads, service_df=None):
        _step = 'Read Services data and append has_jcomm, has_sidecar field to lead data'
        try:
            if service_df is not None:
                df_service_jcomm_sidecar = service_df
            else:
                df_service_jcomm_sidecar = IO.read_csv(self.mode, {'file_dir':
                                                                       self.config['file']
                                                                       ['dir_results'] +
                                                                       self.config['file'][
                                                                           'dir_intermediate'],
                                                                   'file_name': self.config['file'][
                                                                       'Processed']
                                                                   ['services']['intermediate']
                                                                   })

            df_service_jcomm_sidecar = df_service_jcomm_sidecar[['SerialNumber', 'Has_JCOMM',
                                                                 'Has_Sidecar']]

            df_leads = df_leads.merge(df_service_jcomm_sidecar, left_on='SerialNumber_M2M',
                                      right_on='SerialNumber', how='left')

            del df_leads['SerialNumber_y']
            df_leads.rename(columns={'SerialNumber_x': 'SerialNumber'}, inplace=True)

            logger.app_success(_step)
            return df_leads
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

    def pipeline_merge_contract_install(self, df_contract, df_install):  # pragma: no cover
        """
        :params df_contract : Processed Contract Data
        :params df_install : Processed Install Base Data
        :return df_install : Merged contract data into the Install Base data.
        """
        _step = "Merging Install Base data with Contracts Data"
        try:
            df_install = df_install.merge(df_contract, left_on="SerialNumber_M2M",
                                          right_on="SerialNumber", how="left")
            logger.app_success(_step)
            return df_install
        except Exception as excp:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excp

    def pipeline_bom(self, df_install):  # pragma: no cover

        # Read : Reference lead opportunities
        _step = "Read : Reference lead opportunities"
        try:
            ref_lead_opp = IO.read_csv(self.mode, {'file_dir': self.config['file']['dir_ref'],
                                                   'file_name':
                                                       self.config['file']['Reference']
                                                       ['lead_opportunities']
                                                   })
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        # Read : Raw BOM data
        _step = "Read raw data : BOM"
        try:
            df_bom = IO.read_csv(self.mode, {'file_dir': self.config['file']['dir_data'],
                                             'file_name': self.config['file']['Raw']['bom']
                                             ['file_name'],
                                             'sep': '\t'
                                             })
            input_format = self.config['database']['bom']['Dictionary Format']
            df_bom = self.format.format_data(df_bom, input_format)
            # df_bom = read_data(db='bom', type_='data', sep='\t')
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        # Merge raw bom data with processed_merge_contract_install dataframe
        _step = 'Merge data: Install and BOM'
        try:
            df_bom = self.pipeline_merge(df_bom, df_install, type_='lead_id')
            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        # TODO: Before Identifying the leads we need to filter the reference lead data
        #  where EOSL and Life__Years are null or empty, implement that logic here.

        # Identify Lead from Part Number TLN and BOM
        try:
            _step = 'Identify Lead for BOM'
            df_leads = self.identify_leads(df_bom, ref_lead_opp)

            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_leads

    def pipeline_merge_lead_services(self, df_leads, df_services=None):
        """
        This functions reads the services intermediate data and join with the leads data to
        generate a date code column.
        """
        _step = "Merging leads and services data to extract date code at component level"
        try:
            if df_services is not None:
                df_services = df_services
            else:
                df_services = IO.read_csv(self.mode,
                                          {'file_dir': self.config['file']['dir_results'] +
                                                       self.config['file'][
                                                           'dir_intermediate'],
                                           'file_name': self.config['file']['Processed']
                                           ['services']['file_name']
                                           })

            df_services = df_services.drop_duplicates(subset=['SerialNumber']).reset_index(
                drop=True)

            unique_component = []
            for i in list(df_services['component'].unique()):
                if "fan" in i.lower():
                    unique_component.append(i.replace("s", "").strip().lower())
                else:
                    unique_component.append(i.strip().lower())

            df_services = df_services[['SerialNumber', 'component', 'ClosedDate']]

            df_services['component'] = df_services['component'].str.lower()

            # Convert to correct date format
            df_services['ClosedDate'] = pd.to_datetime(df_services['ClosedDate']).dt.strftime(
                '%Y-%m-%d')

            # Convert to correct date format
            df_leads['InstallDate'] = pd.to_datetime(df_leads['InstallDate']).dt.strftime(
                '%Y-%m-%d')

            ls_uni = df_leads['Component'].unique()
            df_leads['Component'] = df_leads['Component'].fillna("")
            # Create a new column based on the list values
            df_leads['temp_column'] = df_leads['Component'].apply(
                lambda x: next((val for val in unique_component if val in x.lower()), x))

            df_leads = df_leads.merge(df_services, left_on=['SerialNumber_M2M', 'temp_column'],
                                      right_on=['SerialNumber', 'component'], how='left')

            # Replace NaN with empty string
            df_leads = df_leads.fillna('')
            df_leads = df_leads.reset_index(drop=True)

            # TODO: Add "source" column to indicate from where the date_code values where generated
            # TODO: either "InstallBase/M2M" or "Services"

            # Use np.where to create the new column
            df_leads['date_code'] = np.where(df_leads['ClosedDate'] != '',
                                             df_leads['ClosedDate'], df_leads['InstallDate'])

            # df_leads.to_csv("./results/temp_output_3.csv")

            df_leads = df_leads.drop_duplicates().reset_index(drop=True)

            logger.app_success(_step)
            return df_leads
        except Exception as excp:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from excp

    def pipeline_merge(self, df_bom, df_install, type_):
        try:
            _step = f'Query install data ({type_}'
            ls_cols = ['Job_Index', 'InstallDate', 'Product_M2M', 'SerialNumber_M2M']
            if type_ == 'lead_id':
                key = 'Job_Index'
                if 'InstallDate' not in df_install.columns:
                    df_install['InstallDate'] = df_install['ShipmentDate'].copy()

                df_install['Product_M2M'] = df_install['Product_M2M'].str.lower()
                # df_install = df_install.drop_duplicates(subset=['Job_Index'])

            elif type_ == 'meta_data':
                key = 'SerialNumber_M2M'
                ls_cols = [col for col in df_install.columns if col not in ls_cols]
                ls_cols = ls_cols + ['SerialNumber_M2M']
                ls_cols.remove('SerialNumber')
                # [
                #     'Job_Index', 'product_type', 'SerialNumber_M2M',
                #     'Shipper_Index', 'ShipperItem_Index',
                #     'ProductClass', 'Prod_vs_Serv', 'Category',
                #     'SO', 'SOLine', 'SOStatus',
                #     'Customer', 'StrategicCustomer',
                #     'ShipTo_Customer', 'ShipTo_Street', 'ShipTo_City', 'ShipTo_State',
                #     'ShipTo_Zip', 'ShipTo_Country',
                #     'SoldTo_Street', 'SoldTo_City',
                #     'SoldTo_State', 'SoldTo_Zip', 'SoldTo_Country',
                #     'Country',
                #     'product_prodclass', 'is_in_usa', 'key_serial', 'key_bom'
                # ]

            df_out = df_bom.merge(df_install[ls_cols], on=key, how='inner')
            # df_out = df_bom.merge(df_install, on=key, how='inner')
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e
        return df_out

    def pipeline_contract_install(self):  # pragma: no cover

        # Read : Contract Processed data
        _step = "Read processed contract data"
        try:
            df_contract = IO.read_csv(self.mode, {'file_dir': self.config['file']['dir_results'] +
                                                              self.config['file'][
                                                                  'dir_intermediate'],
                                                  'file_name': self.config['file']['Processed'][
                                                      'contracts']['file_name']})

            df_contract = df_contract.drop_duplicates(subset=['SerialNumber_M2M']) \
                .reset_index(drop=True)

            df_contract = df_contract.dropna(subset=['Product_M2M'])

            # df_install = read_data(db='processed_install', type_='processed')
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_contract

    # %% ***** Lead Identification *****

    def identify_leads(self, df_bom, ref_lead_opp): # pragma: no cover

        ls_cols_ref = [
            '',
            'Match', 'Component', 'Component_Description', 'End of Prod', 'Status',
            'Life__Years', 'EOSL', 'flag_raise_in_gp']
        ls_cols = ['Job_Index', 'Total_Quantity', '', 'InstallDate',
                   'SerialNumber_M2M', 'key_value']

        # Identify Leads: TLN
        _step = "Identify Leads: TLN"
        try:
            # Lead_id_bases is changed from PartNumber_TLN_BOM
            lead_id_basedon = 'Product_M2M'

            # TODO : based on Product_M2M if (pdu, sts, rpp) then life_in_years should be
            # TODO : life_in_years should be populate with
            # Subset data : Ref
            ls_cols_ref[0] = lead_id_basedon

            # filter reference lead dataframe where value in Product_M2M is empty or na
            ref_lead = ref_lead_opp.loc[
                pd.notna(ref_lead_opp[ls_cols_ref[0]]), ls_cols_ref]

            # ref_lead = ref_lead[ref_lead[ls_cols_ref[0]].isin(['PDU', 'RPP', 'STS'])]
            # ref_lead.loc[:, 'Match'] = "exact"

            # filter rows from reference lead dataframe where Product_M2M value is "blank"
            ref_lead = ref_lead[~ref_lead[lead_id_basedon].isin(['blank'])]

            # Subset data : BOM
            ls_cols[2] = lead_id_basedon

            # dropping duplicates on reference lead and bom_install dataframe
            df_data = df_bom[ls_cols[:-1]].drop_duplicates()
            ref_lead = ref_lead.drop_duplicates(subset=[lead_id_basedon, 'Component']) \
                .reset_index(drop=True)
            df_data = df_bom.drop_duplicates(subset=[lead_id_basedon, 'SerialNumber_M2M']) \
                .reset_index(drop=True)
            df_data['key_value'] = df_data[lead_id_basedon]

            # Id Leads
            df_leads_tln = self.id_leads_for_partno(df_data, ref_lead, lead_id_basedon)

            del df_data, ref_lead

            logger.app_info("Leads After PartNumber_M2M is :" + str(df_leads_tln.shape))

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        # Identify Leads: BOM
        _step = "Identify Leads: BOM"
        try:
            lead_id_basedon = 'PartNumber_BOM_BOM'

            # Subset data : Ref
            ls_cols_ref[0] = lead_id_basedon
            ref_lead = ref_lead_opp.loc[
                pd.notna(ref_lead_opp[ls_cols_ref[0]]), ls_cols_ref]

            # Subset data : BOM
            ls_cols[2] = lead_id_basedon
            df_data = df_bom[ls_cols[:-1]].drop_duplicates()
            df_data['key_value'] = df_data[lead_id_basedon]

            # Id Leads
            df_leads_bom = self.id_leads_for_partno(df_data, ref_lead, lead_id_basedon)

            del df_data, ref_lead
            logger.app_info("Leads After PartNumber_BOM_BOM is :" + str(df_leads_bom.shape))

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        # Merge leads
        _step = "Merge Leads from TLN and BOM"
        try:
            df_leads_out = pd.concat([df_leads_tln, df_leads_bom])
            del df_leads_tln, df_leads_bom

            if self.mode == 'local':
                df_leads_out.to_csv('./results/b4_classify_leads.csv', index=False)

            logger.app_debug(
                f'No of leads b4 classify: {df_leads_out.shape[0]}')
            df_leads_out = self.classify_lead(df_leads_out)
            logger.app_debug(
                f'No of leads after classify: {df_leads_out.shape[0]}')

            if self.mode == 'local':
                df_leads_out.to_csv('./results/aft_classify_leads.csv', index=False)
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_leads_out

    def id_leads_for_partno(self, df_data, ref_lead, lead_id_basedon):

        """

        Reference file identifies the leads based on TLN i.e. assembly level parts. Only one
        pattern exists i.e. bigns_with. In future  if new patterns are identified,
        code would require changes.

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
                'EOSL', 'flag_raise_in_gp', 'SerialNumber_M2M']

            # Prep reference file
            # rename 'PartNumber_TLN_BOM' or "PartNumber_BOM_BOM" column to "key"
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

            # Meta Data,
            # add a column called as "lead_id_basedon" which indicate the lead was calculated
            # on what type of column either PartNumber_TLN_BOM or PartNumber_BOM_BOM
            df_out['lead_id_basedon'] = lead_id_basedon
            logger.app_debug(f'{_step} : SUCCEEDED', 1)

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e
        return df_out

    # ***** Match ref data for lead identification *****
    def lead4_exact_match(self, df_temp_data, df_ref_sub, lead_id_basedon, ls_col_out):

        _step = f'identify leads key: {lead_id_basedon}, match: exact'

        try:
            logger.app_info(f'{_step}: STARTED')
            org_size = df_temp_data.shape[0]
            ls_col_in = df_temp_data.columns
            logger.app_debug(
                f'Data Size Original : {df_temp_data.shape[0]}; ')

            df_temp_data['key'] = df_temp_data[lead_id_basedon].copy()
            df_temp_data = df_temp_data.drop_duplicates()

            df_ref_sub = df_ref_sub.drop_duplicates(subset=['key'])

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

    def lead4_begins_with(self, df_temp_data, df_ref_sub, lead_id_basedon, ls_col_out):

        _step = f'identify leads key: {lead_id_basedon}, match: being_with'
        try:
            logger.app_info(f'{_step}: STARTED')
            org_size = df_temp_data.shape[0]
            df_ref_sub = df_ref_sub.drop_duplicates(subset=['key'])

            df_out_sub = pd.DataFrame()
            ls_key_len = df_ref_sub.len_key.unique()
            ls_col_in = df_temp_data.columns

            logger.app_debug(
                f'Data Size Original : {df_temp_data.shape[0]}; ')

            for key_len in ls_key_len:
                # key_len = ls_key_len[0]
                org_size = df_temp_data.shape[0]

                df_temp_data['key'] = df_temp_data[lead_id_basedon].apply(
                    lambda x: x[:key_len])

                df_decoded = df_temp_data.merge(df_ref_sub, on='key', how='left')

                # Conslidated leads. For TLN where lead has been identififed,
                # will be added to output

                if any(pd.notna(df_decoded.Component)):
                    df_cur_out = df_decoded.loc[
                        pd.notna(df_decoded.Component), ls_col_out]
                else:
                    df_cur_out = pd.DataFrame()
                df_out_sub = pd.concat([df_out_sub, df_cur_out])

                # Filterout data from further processing for keys with lead identified

                df_temp_data = df_decoded.loc[
                    pd.isna(df_decoded.Component), ls_col_in]

                # Cross checking
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

    def lead4_contains(self, df_temp_data, df_ref_sub, lead_id_basedon, ls_col_out):

        _step = f'identify leads key: {lead_id_basedon}, match: contains'
        try:
            logger.app_info(f'{_step}: STARTED')
            org_size = df_temp_data.shape[0]
            df_ref_sub = df_ref_sub.drop_duplicates(subset=['key'])

            df_out_sub = pd.DataFrame()
            ls_key = df_ref_sub['key'].unique()
            ls_col_in = df_temp_data.columns

            logger.app_debug(
                f'Data Size Original : {df_temp_data.shape[0]}; ')

            for key in ls_key:
                # key = ls_key[0]
                org_size = df_temp_data.shape[0]

                df_temp_data['flag_valid'] = df_temp_data[
                    lead_id_basedon].str.contains(key)

                # Conslidated leads. For TLN where lead has been identififed,
                # will be added to output

                if any(pd.notna(df_temp_data.flag_valid)):
                    df_ref_lukup = df_ref_sub[df_ref_sub.key == key]

                    df_cur_out = df_temp_data[df_temp_data['flag_valid']].copy()
                    df_temp_data['key'] = df_temp_data[lead_id_basedon]
                    df_cur_out['left_key'] = key

                    df_cur_out = df_cur_out.merge(
                        df_ref_lukup, how='left',
                        left_on='left_key', right_on='key')
                else:
                    df_cur_out = pd.DataFrame()
                df_out_sub = pd.concat([df_out_sub, df_cur_out[ls_col_out]])

                # Filterout data from further processing for keys with lead identified
                df_temp_data = df_temp_data.loc[
                    df_temp_data.flag_valid == False, ls_col_in]

                # Cross checking
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

        :param df_leads_wn_class: BOM data mapped with lead opprtunities
        :type df_leads_wn_class: pandas dataframe
        :raises Exception: DESCRIPTION
        :return: leads classified as EOSL or Life.
        :rtype: pandas dataframe

        """
        _step = 'Lead classification'
        try:
            df_leads = pd.DataFrame()

            # Merge generated leads and services data
            try:
                _step = 'Merge lead and services data'
                if test_services is None:
                    df_leads_wn_class = self.pipeline_merge_lead_services(df_leads_wn_class)
                logger.app_success(_step)
            except Exception as e:
                logger.app_fail(_step, f"{traceback.print_exc()}")
                raise Exception from e

            # Derive fields
            df_leads_wn_class.loc[:, 'date_code'] = pd.to_datetime(
                df_leads_wn_class['date_code'])
            df_leads_wn_class.loc[:, 'today'] = pd.to_datetime(pd.Timestamp.now())

            df_leads_wn_class['age'] = (
                    (df_leads_wn_class['today'] - df_leads_wn_class['date_code'])
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

            # For DCPD leads due this year will be processed
            df_leads_wn_class.Life__Years = pd.to_numeric(df_leads_wn_class.Life__Years)
            flag_lead_life = pd.notna(df_leads_wn_class.Life__Years)
            if any(flag_lead_life):
                df_leads_sub = df_leads_wn_class[flag_lead_life].copy()
                df_leads_sub['Life__Years'] = df_leads_sub['Life__Years'].replace('', pd.NA)
                df_leads_sub['Life__Years'] = pd.to_numeric(df_leads_sub['Life__Years'],
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


if __name__ == "__main__":
    obj = LeadGeneration()
    obj.main_lead_generation()
