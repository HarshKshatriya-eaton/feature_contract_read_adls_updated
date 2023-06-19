
"""@file



@brief Convert range of SerialNumber to uniqu SerialNumber


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****

import re
import sys
import pandas as pd
import traceback
import numpy as np
from utils import AppLogger

logger = AppLogger(__name__)

class SerialNumber:

    def __init__(self, f_reset=False):

        if f_reset is False:
            try:
                ref_data = pd.read_csv('./src/expanded_serial_number')
            except Exception as e:
                ref_data = pd.DataFrame(columns=[
                    'SerialNumberOrg', 'SerialNumber'])
        else:
            ref_data = pd.DataFrame(columns=[
                'SerialNumberOrg', 'SerialNumber'])

        self.ref_data = ref_data.copy()

    def validate_srnum(self, ar_serialnum):
        """

        :param ar_serialnum: DESCRIPTION
        :type ar_serialnum: TYPE
        :return: DESCRIPTION
        :rtype: TYPE

        """

        df_data = pd.DataFrame(data={'SerialNumber': ar_serialnum})

        # Should not contain
        pat_invalid_self = [
            'bcb', 'bcms', 'box', 'cab', 'com', 'cratin',
            'ext', 'floor', 'freig', 'inst',
            'jbox', 'label', 'line', 'loadbk', 'repo',
            'serv', 'skirts', 'spare', 'su', 'super',
            'test', 'time', 'trai', 'trans', 'tst',
            'warran',
            'nan',

             'bus', 'combos', 'ds',
             'ef', 'ew',
             'fl', 'flsk', 'fs', 'fsk',
             'jb', 'lg', 'lll',
             'misc', 'nin',
             'pm', 'rpp',
             'sk', 'skirt', 'skt', 'spg', 'supk',
             'tap', 'tm', 'tspk'

             'exp', 'fwt']

        pat_invalid_self = "(" + '|'.join(pat_invalid_self) + ")"
        df_data.loc[:, 'f_valid_content'] = df_data['SerialNumber'].apply(
            lambda x: re.search(pat_invalid_self, str(x)) == None)
        del pat_invalid_self

        # Should not end in
        pat_invalid_self = [
            'bus', 'combos', 'ds',
            'ef', 'ew',
            'fl', 'flsk', 'fs', 'fsk',
            'jb', 'lg', 'lll',
            'misc', 'nin',
            'pm', 'rpp',
            'sk', 'skirt', 'skt', 'spg', 'supk',
            'tap', 'tm', 'tspk']
        pat_invalid_self = "(" + "$|".join(pat_invalid_self) + "$)"

        df_data.loc[:, 'f_valid_end'] = df_data['SerialNumber'].apply(
            lambda x: re.search(pat_invalid_self, str(x)) == None)

        df_data['f_valid'] = (
            df_data['f_valid_end'] & df_data['f_valid_content'])

        return df_data['f_valid']

    def prep_srnum(self, df_input):
        col = 'SerialNumberOrg'

        # PreProcess Serial Numbers
        pat_punc = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~ '
        df_input[col] = df_input[col].str.lstrip(pat_punc).str.rstrip(pat_punc)
        df_input[col] = df_input[col].str.replace(' ', '')
        df_input[col] = df_input[col].str.replace('&', ',')


        return df_input[col]

    def get_serialnumber(self, ar_serialnum, ar_installsize, data_type='m2m'):
        self.data_type = data_type

        df_org = pd.DataFrame(data={
            'SerialNumberOrg': ar_serialnum,
            'InstallSize': ar_installsize})

        ref_data = self.ref_data

        # Check if decoded before
        if False:
            ls_decoded = ref_data['SerialNumberOrg'].unique()
            df_org.loc[:, 'known_sr_num'] = df_org['SerialNumberOrg'].apply(
                lambda x: x in ls_decoded)

            # Known ranges
            df_subset = df_org.loc[df_org['known_sr_num'], [
                'SerialNumberOrg', 'InstallSize']].copy()
            df_out_known = self.known_range(df_subset)
            del df_subset
        else:
            df_org['known_sr_num'] = False

        # UnKnown ranges
        df_subset = df_org.loc[df_org['known_sr_num']
                               == False, ['SerialNumberOrg', 'InstallSize']]
        df_out_unknown, df_could_not = self.unknown_range(df_subset)
        del df_subset

        # Club Data
        if False:
            df_out = pd.concat([df_out_known, df_out_unknown])
        else:
            df_out = df_out_unknown.copy()

        return df_out, df_could_not

    def known_range(self, df_input):
        try:
            ref_data = self.ref_data

            ls_cols = ['SerialNumberOrg', 'SerialNumber']
            df_out_known = df_input.merge(
                ref_data[ls_cols], on='SerialNumberOrg', how='right')
        except:
            df_out_known = pd.DataFrame(
                columns=['SerialNumber', 'SerialNumberOrg'])
        return df_out_known

    def unknown_range(self, df_input):
        ls_results = [
            'f_analyze', 'type', 'ix_beg', 'ix_end', 'pre_fix', 'post_fix']

        # Clean Serial Number
        df_input['SerialNumber'] = self.prep_srnum(df_input)

        # Identify Type of Sequence:
        cols = ['SerialNumberOrg', 'InstallSize']
        df_input['out'] = df_input[cols].apply(
            lambda x: self.identify_seq_type(x), axis=1)

        ix = 0
        for col in ls_results:
            df_input[col] = df_input['out'].apply(lambda x: x[ix])
            ix += 1

        # Generate Sequence
        ls_out_unknown = df_input[
            ['out', 'SerialNumberOrg', 'InstallSize']].apply(
                lambda x: self.generate_seq(x[0], x[1], x[2]), axis=1)

        df_out_unknown = pd.concat(ls_out_unknown.tolist())

        could_not = df_input.loc[df_input['f_analyze'] == False, :]

        return df_out_unknown, could_not

    def generate_seq_list(self, dict_data):

        temp_sr = str.split(dict_data['ix_end'], ',')
        rge_sr_num = []
        for sr in temp_sr:
            if '-' not in sr:
                rge_sr_num = rge_sr_num + [int(sr)]
            else:
                split_sr = sr.split('-')
                rge_sr_num = rge_sr_num + list(
                    range(int(split_sr[0]), int(split_sr[1])+1)
                )
        return rge_sr_num

    def generate_seq(self, out, sr_num, size):
        # out = [True] + list(dict_out.values())

        df_out = pd.DataFrame(columns=['SerialNumberOrg', 'SerialNumber'])
        try:
            ls_out_n = ['f_analyze', 'type', 'ix_beg',
                        'ix_end', 'pre_fix', 'post_fix']
            dict_data = dict(zip(ls_out_n, out))

            if dict_data['type'] == 'list':
                rge_sr_num = self.generate_seq_list(dict_data)

                if len(rge_sr_num) < size:
                    temp_sr = str.split(dict_data['pre_fix'], '-')
                    dict_data['pre_fix'] = '-'.join(temp_sr[:-1])
                    dict_data['ix_end'] = temp_sr[-1] + \
                        '-' + dict_data['ix_end']
                    rge_sr_num = self.generate_seq_list(dict_data)

            if dict_data['type'] in ['num', 'num_count']:
                rge_sr_num = range(
                    int(dict_data['ix_beg']), int(dict_data['ix_end'])+1)

            if dict_data['type'] == 'alpha':
                count_sr = (
                    (self.identify_index(dict_data['ix_end']) -
                     self.identify_index(dict_data['ix_beg'])) + 1)
                rge_sr_num = self.letter_range(
                    dict_data['ix_beg'], count_sr)

            ls_srnum = [(dict_data['pre_fix'] + str(ix_sr) +
                        dict_data['post_fix']) for ix_sr in rge_sr_num]
        except:
            ls_srnum = []

        if ((len(ls_srnum) > size)
            and (len(ls_srnum) > 100)
                and (self.data_type == 'm2m')):
            logger.app_debug(
                f'{sr_num}: {len(ls_srnum)} > {size}', 1)
            ls_srnum = []
        elif ((len(ls_srnum) > size) #size
              and (len(ls_srnum) > 150)
              and (self.data_type == 'contract')):
            logger.app_debug(
                f'{sr_num}: {len(ls_srnum)} > {size}', 1)
            ls_srnum = []

        df_out['SerialNumber'] = ls_srnum
        df_out['SerialNumberOrg'] = [sr_num] * len(ls_srnum)

        return df_out

    def identify_seq_type(self, vals):
        # vals = ['12017004-51-59,61', 10]      vals = ['110-1900-12,14,17,19', 4]
        sr_num = vals[0]
        install_size = vals[1]

        f_analyze = True
        dict_out = {'type': '', 'ix_beg': '',
                    'ix_end': '', 'pre_fix': '', 'post_fix': ""}

        sr_num = str.replace(sr_num, '/', '-')
        sr_num = str.replace(sr_num, '--', '-')
        split_sr_num = str.split(str(sr_num), '-')

        if (len(split_sr_num) == 2) and (',' not in sr_num):
            dict_out['type'] = 'num_count'
            dict_out['ix_beg'] = 1
            dict_out['ix_end'] = install_size
            dict_out['pre_fix'] = sr_num + '-'
            ls_out = [True] + list(dict_out.values())
            return ls_out

        if len(split_sr_num) < 2:
            ls_out = [False] + list(dict_out.values())  # TODO: Vipul
            return ls_out

        ix_beg, ix_end = split_sr_num[-2], split_sr_num[-1]
        if len(split_sr_num[:-2]) > 0:
            pre_fix = '-'.join(split_sr_num[:-2]) + '-'
        else:
            pre_fix = ""

        try:
            if ',' in sr_num:
                dict_out['type'] = 'list'
                split_sr_num = str.split(str(sr_num), ',')
                temp_str = str.split(str(split_sr_num[0]), '-')

                if split_sr_num[0].count("-") in [2, 3]:
                    pre_fix = '-'.join(temp_str[:2])
                    ix_end = '-'.join(temp_str[2:])
                    ix_end = ','.join([ix_end] + split_sr_num[1:])

                    ix_beg = ''
                elif split_sr_num[0].count("-") in [1]:
                    pre_fix = temp_str[0]
                    ix_end = ','.join([temp_str[1]] + split_sr_num[1:])
                else:
                    f_analyze = False
                    print(f'Issue: {sr_num}')

            elif ix_beg.isalpha() & ix_end.isalpha():
                dict_out['type'] = 'alpha'
            elif ix_beg.isdigit() & ix_end.isdigit():
                dict_out['type'] = 'num'

            elif ix_beg.isdigit() & ix_end.isalnum():
                dict_out['type'] = 'num'

                loc_split = [ix for ix in range(
                    len(ix_end)) if (ix_end[ix].isdigit())]
                dict_out['post_fix'] = ix_end[max(loc_split)+1:]
                ix_end = ix_end[:max(loc_split)+1]

            elif ix_beg.isalnum() & ix_end.isalpha():
                dict_out['type'] = 'alpha'
                loc_split = [ix for ix in range(
                    len(ix_beg)) if (ix_beg[ix].isdigit())]
                pre_fix = pre_fix + ix_beg[:max(loc_split)+1]
                ix_beg = ix_beg[max(loc_split)+1:]

            elif ix_beg.isalnum() & ix_end.isalnum():
                loc_split_end = [ix for ix in range(
                    len(ix_end)) if (ix_end[ix].isdigit())]
                loc_split_beg = [ix for ix in range(
                    len(ix_beg)) if (ix_beg[ix].isdigit())]

                if ix_end[max(loc_split_end)+1:] == ix_beg[max(loc_split_beg)+1:]:
                    dict_out['type'] = 'num'
                    dict_out['post_fix'] = ix_end[max(loc_split_end)+1:]

                    ix_end = ix_end[:max(loc_split_end)+1]
                    ix_beg = ix_beg[:max(loc_split_beg)+1]
                else:
                    f_analyze = False
            else:
                f_analyze = False
        except:
            return [False] + list(dict_out.values())

        if f_analyze:
            dict_out['pre_fix'] = pre_fix
            dict_out['ix_beg'] = ix_beg
            dict_out['ix_end'] = ix_end

        return [f_analyze] + list(dict_out.values())

    def letter_range(self, seq_, size):
        pwr = len(seq_)
        ix = self.identify_index(seq_)

        ls_sr_num = []
        for ix_sr in range(ix, ix+size):
            # ix_sr = list(range(ix, ix+size))[0]
            srnum = self.convert_index(ix_sr, pwr)
            ls_sr_num.append(srnum)

        return ls_sr_num

    def identify_index(self, seq_):
        # seq_ = 'ba'
        val_total = 0
        for pos in list(range(len(seq_))):
            # pos = 1
            ix_aplha = (ord(seq_[pos]) - 96)
            pwr_alpha = len(seq_) - pos - 1
            val_aplha = (26**pwr_alpha) * ix_aplha
            val_total = val_total + val_aplha
        return val_total

    def convert_index(self, ix_n, pwr):
        total_txt = ''
        for loc in list(range(pwr, 0, -1)):
            # loc = list(range(pwr, 0, -1))[1]
            val = ix_n // (26**(loc-1))
            total_txt = total_txt + chr(96+val)
            ix_n = ix_n % (26**(loc-1))
        return total_txt


# %%
if __name__ == 'main':
    sr_num = SerialNumber(f_reset=True)
    ar_serialnum = [
        # num
        '180-0557-1-2b', '180-0557-1-2', '180-1059-1-24-fl', '180-1223-1-12-fs',
        '560-0152-4-8', '560-0153-5-17', '110-1540-1-6',
        # alpha
        '180-0557-a-q', '180-05578a-q',
        # list
        '180-0557-a,b,c', '180-0557a,b']

    ar_installsize = [2, 2, 24, 12, 5, 13, 6, 17, 17, 3, 2]

    df_data = pd.read_csv('./data/SerialNumber.csv')
    df_data['f_include'] = sr_num.validate_srnum(df_data['Serial #'])
    df_data = df_data[df_data['f_include']]

    from src.class_business_logic import BusinessLogic
    cbl = BusinessLogic()
    df_data['f_include'] = cbl.idetify_product_fr_serial(df_data['Serial #'])
    df_data['f_include'] = df_data['f_include'].isin(['STS','RPP','PDU'])
    df_data = df_data[df_data['f_include']]

    df_out_srs, df_out_couldnot = sr_num.get_serialnumber(
        df_data['Serial #'], df_data['Shipper Qty'])

    #df_out_srs, df_out_couldnot = sr_num.get_serialnumber(ar_serialnum, ar_installsize)
    df_out_srs.to_csv('./results/output1.csv')

# %%