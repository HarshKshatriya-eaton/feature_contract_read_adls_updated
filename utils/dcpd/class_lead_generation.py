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

    def main_lead_generation(self):

        # # ***** Read Processed Install Data *****
        # _step = 'Read Processed Install data'
        # try:
        #     df_install = self.pipeline_installbase()
        #     logger.app_success(_step)
        # except Exception as excp:
        #     logger.app_fail(_step, f'{traceback.format_exc()}')
        #     raise Exception('f"{_step}: Failed') from excp

        _step = 'Read Merged Contracts and Install Base data'
        try:
            df_install = self.pipeline_contract_install()
            logger.app_success(_step)
        except Exception as excp:
            logger.app_fail(_step, f'{traceback.format_exc()}')
            raise Exception('f"{_step}: Failed') from excp

        # # ***** Merge Processed Contract and Install Base Data *****
        # _step = 'Merge Contract and Install Base Data'
        # try:
        #     df_install = self.pipeline_merge_contract_install(df_contract, df_install)
        #     logger.app_success(_step)
        # except Exception as excp:
        #     logger.app_fail(_step, f'{traceback.format_exc()}')
        #     raise Exception('f"{_step}: Failed') from excp

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

        try:
            IO.write_csv(self.mode, {'file_dir': self.config['file']['dir_results'],
                                     'file_name': self.config['file']['Processed']['output_iLead'][
                                         'file_name']
                                     }, df_leads)
            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return 'successfull !'

    # %% ***** Pipelines *****

    def pipeline_merge_contract_install(self, df_contract, df_install):
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
            raise Exception('f"{_step}: Failed') from e


    def pipeline_bom(self, df_install):

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

        # Merge Data
        try:
            _step = 'Merge data: Install and BOM'
            df_bom = self.pipeline_merge(df_bom, df_install, type_='lead_id')
            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        # TODO: Before Identifying the leads we need to filter the reference lead data
        #  where EOSL and Life__Years are null or empty, implement that logic here.

        # Identify Lead from Part Number
        try:
            _step = 'Identify Lead for BOM'
            df_leads = self.identify_leads(df_bom, ref_lead_opp)

            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        # Merge generated leads and services data
        try:
            _step = 'Merge lead and services data'
            # df_leads = self.pipeline_leads_services(df_leads)

            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_leads

    def pipeline_leads_services(self, df_lead):
        """
        This functions reads the services intermediate data and join with the leads data to
        generate a date code column.
        """

    def pipeline_merge(self, df_bom, df_install, type_):
        try:
            _step = f'Query install data ({type_}'
            key = 'Job_Index'
            if type_ == 'lead_id':
                ls_cols = ['Job_Index', 'InstallDate', 'Product_M2M', 'SerialNumber_M2M']

                if 'InstallDate' not in df_install.columns:
                    df_install['InstallDate'] = df_install['ShipmentDate'].copy()

                df_install = df_install.drop_duplicates(subset=['Job_Index'])

            elif type_ == 'meta_data':

                ls_cols = [
                    'Job_Index',
                    'SerialNumber_M2M', 'Product_M2M', 'Description', 'product_type',
                    'Shipper_Index', 'ShipperItem_Index',
                    'ProductClass', 'Prod_vs_Serv', 'Category',
                    'SO', 'SOLine', 'SOStatus',
                    'Customer', 'StrategicCustomer',
                    'ShipTo_Customer', 'ShipTo_Street', 'ShipTo_City', 'ShipTo_State',
                    'ShipTo_Zip', 'ShipTo_Country',
                    'SoldTo_Street', 'SoldTo_City',
                    'SoldTo_State', 'SoldTo_Zip', 'SoldTo_Country',
                    'Country',
                    'product_prodclass', 'is_in_usa', 'key_serial', 'key_bom'
                ]

            df_out = df_bom.merge(df_install[ls_cols], on=key, how='inner')
            # df_out = df_bom.merge(df_install, on=key, how='inner')
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e
        return df_out

    # def pipeline_installbase(self):
    #
    #     # Read : Installbase Processed data
    #     _step = "Read raw data : BOM"
    #     try:
    #         df_install = IO.read_csv(self.mode, {'file_dir': self.config['file']['dir_data'],
    #                                              'file_name': self.config['file']['Processed'][
    #                                                  'processed_install'][
    #                                                  'file_name']})
    #
    #         # df_install = read_data(db='processed_install', type_='processed')
    #     except Exception as e:
    #         logger.app_fail(_step, f'{traceback.print_exc()}')
    #         raise Exception('f"{_step}: Failed') from e
    #
    #     return df_install

    def pipeline_contract_install(self):

        # Read : Contract Processed data
        _step = "Read processed contract data"
        try:
            df_contract = IO.read_csv(self.mode, {'file_dir': self.config['file']['dir_results'] +
                                                             self.config['file']['dir_intermediate'],
                                                 'file_name': self.config['file']['Processed'][
                                                     'contracts']['merge_install']})

            # df_install = read_data(db='processed_install', type_='processed')
        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        return df_contract

    # %% ***** Lead Identification *****

    def identify_leads(self, df_bom, ref_lead_opp):

        ls_cols_ref = [
            '',
            'Match', 'Component', 'Description', 'End of Prod', 'Status',
            'Life__Years', 'EOSL', 'flag_raise_in_gp']
        ls_cols = ['Job_Index', 'Total_Quantity', '', 'InstallDate', 'Product_M2M',
                   'SerialNumber_M2M', 'key_value']

        # Identify Leads: TLN
        _step = "Identify Leads: TLN"
        try:
            lead_id_basedon = 'PartNumber_TLN_BOM'

            # Subset data : Ref
            ls_cols_ref[0] = lead_id_basedon
            ref_lead = ref_lead_opp.loc[
                pd.notna(ref_lead_opp[ls_cols_ref[0]]), ls_cols_ref]

            # Subset data : BOM
            ls_cols[2] = lead_id_basedon
            df_data = df_bom[ls_cols[:-1]].drop_duplicates()
            df_data['key_value'] = df_data[lead_id_basedon]

            # Id Leads
            df_leads_tln = self.id_leads_for_partno(df_data, ref_lead, lead_id_basedon)

            del df_data, ref_lead

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

        except Exception as e:
            logger.app_fail(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from e

        # Merge leads
        _step = "Identify Leads: BOM"
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

            ls_col_out = [
                'Job_Index', 'Total_Quantity',
                'Component', 'Description',
                'key', 'End of Prod', 'Status', 'Life__Years',
                'EOSL', 'flag_raise_in_gp', 'Product_M2M', 'SerialNumber_M2M']

            # Prep reference file
            ref_lead = ref_lead.rename(
                columns={ref_lead.columns[0]: 'key'})
            ref_lead['len_key'] = ref_lead.key.str.len()
            ref_lead = ref_lead.sort_values(by=['len_key'], ascending=False)
            ref_lead['key'] = ref_lead['key'].str.lower()

            ls_col_out += ['InstallDate', 'key_value']

            # Identify leads where PartNumber matches exactly.
            # if lead is identified for PartNumber;
            # then it will be dropped from further processing

            if 'exact' in types_patterns:
                df_ref_sub = ref_lead[ref_lead.Match == 'exact']
                df_cur_out, df_data = self.lead4_exact_match(
                    df_data, df_ref_sub, lead_id_basedon, ls_col_out)

                if df_cur_out.shape[0] > 0:
                    df_cur_out.loc[:, 'lead_match'] = 'exact'

                    df_out = pd.concat([df_out, df_cur_out])
                del df_ref_sub, df_cur_out

            if 'contains' in types_patterns:
                df_ref_sub = ref_lead[ref_lead.Match == 'contains']
                df_cur_out, df_data = self.lead4_contains(
                    df_data, df_ref_sub, lead_id_basedon, ls_col_out)

                if df_cur_out.shape[0] > 0:
                    df_cur_out.loc[:, 'lead_match'] = 'contains'

                    df_out = pd.concat([df_out, df_cur_out])
                del df_ref_sub, df_cur_out

            if 'begins_with' in types_patterns:
                df_ref_sub = ref_lead[ref_lead.Match == 'begins_with']
                df_cur_out, df_data = self.lead4_begins_with(
                    df_data, df_ref_sub, lead_id_basedon, ls_col_out)

                if df_cur_out.shape[0] > 0:
                    df_cur_out.loc[:, 'lead_match'] = 'begins_with'
                    df_out = pd.concat([df_out, df_cur_out])
                del df_ref_sub, df_cur_out

            # Meta Data
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

            # Conslidated leads. For TLN where lead has been identififed,
            # will be added to output

            if any(pd.notna(df_decoded.Component)):
                df_out_sub = df_decoded.loc[
                    pd.notna(df_decoded.Component), ls_col_out]
            else:
                df_out_sub = pd.DataFrame()

            # df_temp_data: Remove keys with identified lead from further processing

            df_temp_data = df_decoded.loc[
                pd.isna(df_decoded.Component), ls_col_in]

            # Cross checking
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

        _step = f'identify leads key: {lead_id_basedon}, match: being_with'
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
    def classify_lead(self, df_leads_wn_class):
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

            # TODO: Read and merge services data with "df_leads_wn_class" dataframe.
            # TODO: Update DataCode column based on installDate and closedDate cols.

            # Derive fields
            df_leads_wn_class.loc[:, 'InstallDate'] = pd.to_datetime(
                df_leads_wn_class['InstallDate'])
            df_leads_wn_class.loc[:, 'today'] = pd.to_datetime(pd.Timestamp.now())
            df_leads_wn_class['age'] = (
                    (df_leads_wn_class['today'] - df_leads_wn_class['InstallDate'])
                    / np.timedelta64(1, 'Y'))

            # EOSL Leads
            flag_lead_eosl = pd.notna(df_leads_wn_class.EOSL)
            if any(flag_lead_eosl):
                df_leads_sub = df_leads_wn_class[flag_lead_eosl].copy()
                df_leads_sub.loc[:, 'lead_type'] = 'EOSL'
                df_leads = pd.concat([df_leads, df_leads_sub])
                del df_leads_sub
            del flag_lead_eosl

            # For DCPD leads due this year will be processed
            flag_lead_life = pd.notna(df_leads_wn_class.Life__Years)
            if any(flag_lead_life):
                df_leads_sub = df_leads_wn_class[flag_lead_life].copy()

                df_leads_sub.loc[:, 'flag_include'] = (
                        df_leads_sub['age'] > df_leads_sub['Life__Years'])

                if any(df_leads_sub.flag_include):
                    df_leads_sub.loc[:, 'lead_type'] = 'Life'
                    df_leads_sub = df_leads_sub[df_leads_sub['flag_include']]
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
