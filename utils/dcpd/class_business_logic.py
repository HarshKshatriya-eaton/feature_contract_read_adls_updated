
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
# %% ***** Setup Environment *****
import json
import os
import re
import sys
import traceback
import pandas as pd
from utils import IO
from utils import AppLogger
import logging
#import utils.json_creator as js
logger = AppLogger(__name__)

class BusinessLogic:

    def __init__(self):
        logger.app_info('inside Business Logic')
        config_dir = os.path.join(os.path.dirname(__file__), "../../config")
        config_file = os.path.join(config_dir, "config_dcpd.json") 
        logger.app_info('config file path fetched')
        # Read the configuration file
        with open(config_file,'r') as config_file:
            config = json.load(config_file)
        #self.config=js.read_json(config_file)
        self.config = config
        logger.app_info("'config':config")
        self.mode = self.config.get("conf.env", "azure-adls")

        logger.app_info('read reference file initiated')
        # Read Reference: Product from Serial Number
        ref_prod_fr_srnum = IO.read_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_ref'],
                 'file_name': self.config['file']['Reference']['decode_sr_num']['file_name'],
                 'adls_config': self.config['file']['Reference']['adls_credentials'],
                 'adls_dir': self.config['file']['Reference']['decode_sr_num']
                 })
        
        # column_mapping = self.config["database"]["Reference"]["ref_decode_serialnumber"]

        # for original_column, database_field in column_mapping.items():
        #     ref_prod_fr_srnum.rename(columns={original_column: database_field}, inplace=True)
            
        # if isinstance(ref_prod_fr_srnum, pd.DataFrame):
        #     logging.info(ref_prod_fr_srnum.columns)
        # else:
        #     logging.error(f"Unexpected type for ref_prod_fr_srnum: {type(ref_prod_fr_srnum)}")

        logger.app_info(f"Type for ref_prod_fr_srnum: {type(ref_prod_fr_srnum)}")
        ref_prod_fr_srnum['SerialNumberPattern'] = ref_prod_fr_srnum['SerialNumberPattern'].str.lower()
        self.ref_prod_fr_srnum = ref_prod_fr_srnum

        # Read Reference: Product from TLN
        logger.app_info("Reading the file ref_lead_opportunities")
        ref_lead_opp = IO.read_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_ref'],
                 'file_name': self.config['file']['Reference']['lead_opportunities']['file_name'],
                 'adls_config': self.config['file']['Reference']['adls_credentials'],
                 'adls_dir': self.config['file']['Reference']['lead_opportunities']
                 })
        logger.app_info(f'ref_lead_opp in business logic: 76: {ref_lead_opp.columns}')
        




    def idetify_product_fr_serial(self, ar_serialnumber):

        df_data = pd.DataFrame(data={'SerialNumber': ar_serialnumber})

        # ref_prod_fr_srnum
        ref_prod = self.ref_prod_fr_srnum
        ref_prod = ref_prod[ref_prod.flag_keep]

        # Pre-Process Data
        ls_prod = ref_prod.Product.unique()
        df_data['Product'] = ''
        for prod in ls_prod:
            # prod = ls_prod [0]
            ls_pattern = ref_prod.loc[
                ref_prod.Product == prod, 'SerialNumberPattern']

            df_data['flag_prod'] = df_data.SerialNumber.str.startswith(
                tuple(ls_pattern))
            df_data.loc[df_data['flag_prod'], 'Product'] = prod
            df_data = df_data.drop('flag_prod', axis=1)

        dict_pat = {'526?-3421': 'STS', '26??-0820': 'RPP'}
        for pat in dict_pat:
            prod = dict_pat[pat]
            df_data['flag_prod'] = df_data.SerialNumber.apply(
                lambda x: re.search(pat, x) != None)
            df_data.loc[df_data['flag_prod'], 'Product'] = prod
            df_data = df_data.drop('flag_prod', axis=1)

        #
        return df_data['Product']

    # def idetify_product_fr_tln(self, ar_tln):
    #     # ar_tln = list(df_bom.TLN_BOM)

    #     # Data to classify
    #     df_data = pd.DataFrame(data={'key': ar_tln})

    #     # Reference Data
    #     ref_prod = self.ref_lead_opp.copy()
    #     ref_prod = ref_prod[
    #         (ref_prod.flag_keep) & (pd.notna(ref_prod.PartNumber_TLN))]

    #     # Pre-Process Data
    #     ls_prod = ref_prod.Product.unique()
    #     df_data['Product'] = ''
    #     for prod in ls_prod:
    #         # prod = ls_prod [0]
    #         ls_pattern = ref_prod.loc[
    #             (ref_prod.Product == prod), 'PartNumber_TLN']

    #         df_data['flag_prod_beg'] = df_data.key.str.startswith(
    #             tuple(ls_pattern))
    #         df_data['flag_prod_con'] = df_data.key.apply(
    #             lambda x: re.search(f'({str.lower(prod)})', x) is not None)
    #         df_data['flag_prod'] = (df_data['flag_prod_beg']
    #                                 | df_data['flag_prod_con'])

    #         df_data.loc[df_data['flag_prod'], 'Product'] = prod
    #         df_data = df_data.drop(
    #             ['flag_prod', 'flag_prod_beg', 'flag_prod_con'], axis=1)

    #     return df_data['Product']



# %%
