
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
# %% ***** Setup Environment *****

import re
import sys
import traceback
import pandas as pd


class BusinessLogic:

    def __init__(self):

        # Read Reference: Product from Serial Number
        ref_prod_fr_srnum = pd.read_csv(
            './references/ref_decode_serialnumber.csv')
        ref_prod_fr_srnum['SerialNumberPattern'] = ref_prod_fr_srnum[
            'SerialNumberPattern'].str.lower()
        self.ref_prod_fr_srnum = ref_prod_fr_srnum

        # Read Reference: Product from TLN
        self.ref_lead_opp = pd.read_csv(
            './references/ref_lead_opportunities.csv')

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

    def idetify_product_fr_tln(self, ar_tln):
        # ar_tln = list(df_bom.TLN_BOM)

        # Data to classify
        df_data = pd.DataFrame(data={'key': ar_tln})

        # Reference Data
        ref_prod = self.ref_lead_opp.copy()
        ref_prod = ref_prod[
            (ref_prod.flag_keep) & (pd.notna(ref_prod.PartNumber_TLN))]

        # Pre-Process Data
        ls_prod = ref_prod.Product.unique()
        df_data['Product'] = ''
        for prod in ls_prod:
            # prod = ls_prod [0]
            ls_pattern = ref_prod.loc[
                (ref_prod.Product == prod), 'PartNumber_TLN']

            df_data['flag_prod_beg'] = df_data.key.str.startswith(
                tuple(ls_pattern))
            df_data['flag_prod_con'] = df_data.key.apply(
                lambda x: re.search(f'({str.lower(prod)})', x) is not None)
            df_data['flag_prod'] = (df_data['flag_prod_beg']
                                    | df_data['flag_prod_con'])

            df_data.loc[df_data['flag_prod'], 'Product'] = prod
            df_data = df_data.drop(
                ['flag_prod', 'flag_prod_beg', 'flag_prod_con'], axis=1)

        return df_data['Product']



# %%
