
"""@file class_contracts_data



@brief : For DCPD business; analyze contracts data from SalesForce to be consumed
by lead generation


@details :
    DCPD has two tables for contracts data (contracts and renewal contracts).
    Code summaries both the datasets to understand if a unit is currently under
    active contract

    1. Contracts: has warranty and startup details

    2. Renewal contract: has contracts data (other than warranty)


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""


# %% ***** Setup Environment *****

from src.class_business_logic import BusinessLogic
import src.config_contract as config_contract
from src.class_serial_number import SerialNumber

import numpy as np
import re
import pandas as pd
import traceback
from string import punctuation
punctuation = punctuation + ' '

if not ('CONF_' in locals()) | ('CONF_' in globals()):
    print(__name__)
    import src.config_set as CONF_

if not ('ENV_' in locals()) | ('ENV_' in globals()):
    print(__name__)

    from src.class_help_setup import SetupEnvironment
    ENV_ = SetupEnvironment('DCPD', CONF_.dict_)

SRNUM = SerialNumber()
BL = BusinessLogic()

# %% ***** Main *****


def main_contracts():
    """
    Main pipeline for contracts and reneawal data.

    :raises Exception: Collects any / all exception.
    :return: message if successful.
    :rtype: string.

    """

    # PreProcess: Contracts Data
    _step = 'PreProcess: Contracts Data'
    try:
        df_contract = pipeline_contract()
        ENV_.logger.app_success(_step)
    except Exception as e:
        ENV_.logger.app_fail(_step, f"{traceback.print_exc()}")
        #raise Exception from e

    # PreProcess : Renewal data
    try:
        _step = 'Process renewal data'
        df_renewal = pipeline_renewal()

        ENV_.logger.app_success(_step)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        #raise Exception('f"{_step}: Failed') from e

    # Merge Datasets
    try:
        _step = 'Merge Data: Contract and Renewal'
        df_contract = merge_contract_and_renewal(
            df_contract, df_renewal)

        df_contract = decode_ContractType(df_contract)

        ENV_.logger.app_success(_step)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        #raise Exception('f"{_step}: Failed') from e

    # Export Data
    _step = 'Export contracts data'
    try:
        if ENV_.ENV == 'local':
            df_contract.to_csv('./results/processed_contract_validation', index=False)

        dict_format = ENV_.CONF['outputformats']['contracts']
        df_contract_form = ENV_.filters_.format_output(df_contract, dict_format)

        ENV_.export_data(df_contract, 'contracts', 'Processed')

        ENV_.logger.app_success(_step)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return 'successfull !'


# %% ***** Pipelines *****

def pipeline_contract():
    """
    Pipeline to pre-process contracts data to:
        - Identify startups
        - Extract serial number from text
        - Validate serial number (serial number should exist in installbase)

    :raises Exception: DESCRIPTION
    :return: processed contracts data with serial numbers.
    :rtype: pandas dataframe.

    """

    # PreProcess : Contracts data
    try:
        # Read raw contracts data
        _step = 'Read raw contracts data'
        df_contract = ENV_.read_data('contracts', 'data')
        df_contract.reset_index(drop=True, inplace=True)
        ENV_.logger.app_success(_step)

        # Identify Startups
        _step = 'Identify Startups'
        ls_cols_startup = config_contract.ls_cols_startup

        df_contract[['was_startedup', 'startup_date']] = id_startup(
            df_contract[ls_cols_startup])
        del ls_cols_startup
        ENV_.logger.app_success(_step)

        # Identify Serial Numbers
        _step = 'Identify Serial Number'
        df_contract_srnum = id_srnum(df_contract)
        ENV_.logger.app_success(_step)

        # Validate Serial number from InstallBase
        _step = 'Validate Serial number'
        df_contract_srnum = validate_srnum(df_contract_srnum)

        if ENV_.ENV == "local":
            df_contract_srnum.to_csv('./results/validated_srnum.csv', index=False)
        df_contract_srnum = df_contract_srnum.loc[
            df_contract_srnum.flag_validinstall, :]

        ENV_.logger.app_success(_step)

        # Merge daya
        _step = 'Merge Data: Contract and SerialNumber'
        df_contract = merge_contract_and_srnum(df_contract, df_contract_srnum)

        ENV_.logger.app_success(_step)

    except Exception as e:

        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_contract


def pipeline_renewal():
    """
     Pipeline to pre-process renewal data.

    :raises Exception: DESCRIPTION
    :return: processed contracts data with serial numbers.
    :rtype: pandas dataframe.

    """

    try:
        _step = 'Read renewal data'
        df_renewal = ENV_.read_data('renewal', 'data')

        _step = 'Preprocess data'
        df_renewal['Contract_Amount'] = df_renewal['Contract_Amount'].fillna(0)

        ENV_.logger.app_success(_step)
    except Exception as e:
        ENV_.logger.app_fail(_step, f"{traceback.print_exc()}")
        raise Exception from e

    return df_renewal


# %% ***** Support Codes : Contract *****

def id_startup(df_startup_org):
    """
    Identify if EATON started up the product.

    :param df_startup_org: Dataframe with possible startup date fields in the sequesce if Priority,
    :type df_startup_org: pandas DataFrame.
    :return: Data Frame with two columns:

        was_startedup : Flag indicating if product has StartUp.
        startup_date : Date when the product is started up.
    :rtype: pandas Data Frame

    """
    _step = f"{' '*5}Identify Start-up"
    try:

        df_startup = df_startup_org.copy()
        del df_startup_org

        ls_cols_startup = df_startup.columns
        for ix in range(len(ls_cols_startup)):
            col = ls_cols_startup[ix]

            if ix == 0:
                df_startup['startup_date'] = df_startup[col]
            else:
                df_startup['startup_date'] = df_startup['startup_date'].fillna(
                    df_startup[col])

            ENV_.logger.app_debug(
                f"# NAs in Startup: "
                f"{pd.isna(df_startup['startup_date']).value_counts()[True]}")

        df_startup['was_startedup'] = pd.notna(df_startup['startup_date'])

        df_startup = df_startup[['was_startedup', 'startup_date']]

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_startup


def validate_srnum(df_contract_srnum):

    _step = 'Validate contract Serial Numbers '
    try:
        df_install = read_processed_installbase()
        df_install.loc[:, 'SerialNumber'] = df_install.SerialNumber_M2M.astype(
            str)

        ls_srnums = df_install.SerialNumber.str.lower()

        concat_ls_srnum = ' ' + ', '.join(ls_srnums)

        ls_srnums = tuple(ls_srnums)
        df_contract_srnum.loc[:, 'flag_validinstall'] = df_contract_srnum[
            'SerialNumber'].apply(
                lambda x: (
                    str.lower(x).startswith(ls_srnums)
                    | (f' {x}' in concat_ls_srnum)))

        if False:
            # regex based search
            ls_pattern = ')|(^'.join(df_install.SerialNumber)
            ls_pattern = '(^' + ls_pattern + ')'

            df_contract_srnum.loc[:, 'flag_validinstall'] = df_contract_srnum[
                'SerialNumber'].apply(lambda x: re.match(ls_pattern, x) != None)

        ENV_.logger.app_success(_step)
    except Exception as e:
        ENV_.logger.app_fail(_step, f"{traceback.print_exc()}")
        raise Exception from e

    return df_contract_srnum


def read_processed_installbase():

    # Read : Installbase Processed data
    _step = "Read raw data : BOM"
    try:
        df_install = ENV_.read_data(db='processed_install', type_='processed')

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_install


def decode_ContractType(df_contract):

    df_temp = df_contract.copy()

    # Decode : Service Plan
    dict_contract = {
        'gold': 'PowerTrust Preferred Plan',
        'silver': 'PowerTrust Plan',
        'bronze': 'Flex TM Response Only Contract + Annual PM',
        'extended warranty':	'Warranty Upgrade'
    }

    df_temp.Service_Plan = df_temp.Service_Plan.str.lower()
    df_temp['Eaton_ContractType'] = ""
    for type_ in dict_contract:
        # type_ = 'Gold'
        df_temp['flag_ided'] = df_temp[
            'Service_Plan'].str.contains(str.lower(type_))
        df_temp['flag_blank'] = (df_temp['Eaton_ContractType'] == "")
        df_temp['flag'] = (df_temp['flag_blank'] & df_temp['flag_ided'])
        df_temp.loc[
            df_temp['flag'], 'Eaton_ContractType'] = dict_contract[type_]
    df_temp = df_temp.drop(['flag_ided', 'flag_blank', 'flag'], axis=1)

    # Decode : Installbase
    df_install_temp = pd.read_csv('./data/M2M_data.csv')
    df_install_temp = df_install_temp[['SO', 'Description']]

    df_install_temp['Description'] = df_install_temp[
        'Description'].str.replace(" ", "").str.lower().str.replace(")", "")
    dict_contract = {
        'PowerTrust Preferred Plan': r"(2preventivemaintenance)",
        'PowerTrust Plan': r"(1preventivemaintenance)",
        'Flex TM Response Only Contract + Annual PM': r"(pmonlyservice)"
    }

    # Decode Contracts from

    df_install_temp.loc[:, 'Eaton_ContractType_M2M'] = ""

    for type_ in dict_contract:
        # type_ = 'gold'
        print(type_)
        pat_contract = dict_contract[type_]
        df_install_temp['flag_ided'] = df_install_temp[
            'Description'].apply(
                lambda x: re.search(pat_contract, str(x)) != None)

        df_install_temp['flag_blank'] = (
            df_install_temp['Eaton_ContractType_M2M'] == "")
        df_install_temp['flag'] = (
            df_install_temp['flag_blank'] & df_install_temp['flag_ided'])

        df_install_temp.loc[
            df_install_temp['flag'], 'Eaton_ContractType_M2M'] = type_

    df_install_temp = df_install_temp.loc[
        df_install_temp['Eaton_ContractType_M2M'] != "",
        ['SO', 'Eaton_ContractType_M2M']]

    df_temp['Original_Sales_Order__c'] = df_temp['Original_Sales_Order__c'].astype(
        str)
    df_install_temp['SO'] = df_install_temp['SO'].astype(str)
    df_temp = df_temp.merge(
        df_install_temp, left_on='Original_Sales_Order__c', right_on='SO', how='left')

    df_temp['flag_update'] = (df_temp['Eaton_ContractType'] == "")

    df_temp.loc[df_temp['flag_update'],
                'Eaton_ContractType'] = df_temp.loc[df_temp['flag_update'], 'Eaton_ContractType_M2M']

    # df_temp = df_temp.drop(['SO', 'Eaton_ContractType_M2M'], axis=1)
    return df_temp


# %% ***** Support Codes : Serial Number *****

def id_srnum(df_contract):
    '''
    Identify Serial Numbers for the contract. PDI SalesForce has multiple
    columns where SerialNumber and corresponding Qty is logged.

    :param df_contract: Contracts data from PDI salesforce.
    :type df_contract: pandas DataFrame
    :return: Serial Numbers extracted from PDI SalesForce.
    :rtype: pandas DataFrame

    '''
    _step = f"{' '*5}Extract Serial Numbers from contract"

    try:
        # Prepare Data
        ls_cols = ['Qty_1__c', 'Qty_2__c', 'Qty_3__c', 'Qty_Total_del__c']
        df_contract[ls_cols] = df_contract[ls_cols].fillna(0)
        df_contract['Qty_comment'] = df_contract[ls_cols].apply(
            lambda x: x[3] - (x[0] + x[1] + x[2]), axis=1)

        # Search Serial Numbers based on pattern for SerialNumber
        dict_srnum_cols = config_contract.dict_srnum_cols

        df_serialnum = search_srnum(df_contract, dict_srnum_cols)
        df_serialnum = df_serialnum[pd.notna(df_serialnum.SerialNumber)]

        # Filter out non serial numbers
        df_serialnum = filter_srnum(df_serialnum)

        # Use serial number as is if its not a arange
        df_serialnum.loc[:, 'is_single'] = flag_serialnumber_wid_range(
            df_serialnum)

        ls_cols = ['ContractNumber', 'SerialNumberContract', 'Qty',
                   'Product', 'SerialNumber', 'src']

        # Serial Number - Single
        df_out_sub_single = df_serialnum.loc[
            df_serialnum['is_single'], ls_cols].copy()

        # Serial Number - Multiple
        df_convert_rge = df_serialnum.loc[
            ~df_serialnum['is_single'], ls_cols].copy()

        df_out_sub_multi = get_range_srum(
            df_convert_rge[ls_cols])

        df_out = pd.concat([df_out_sub_single, df_out_sub_multi])
        df_out['SerialNumber'] = df_out['SerialNumber'].fillna(
            df_out['SerialNumberOrg'])

        df_out = df_out.drop_duplicates(subset=['SerialNumber'])

        # TODO: Remove in prod
        df_out.to_csv('./results/contract_SrNumbers.csv', index=False)

        ENV_.logger.app_success(_step)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_out


def search_srnum(df_temp_org, dict_srnum_cols):
    """
    Contracts data with SerialNumbers.

    :param df_temp_org: DESCRIPTION
    :type df_temp_org: pandas DataFrame
    :raises Exception: DESCRIPTION
    :return: Contracts data with extracted SerialNumbers:

    :rtype: pandas DataFrame

    """
    _step = "Extrat Serial Number from fields"

    try:
        df_temp = df_temp_org.copy()
        del df_temp_org

        # Input
        sep = ' '

        # Initialize Output
        df_serialnum = pd.DataFrame()

        # PDI Salesforce has 4 fields with SerialNumber data.
        # Extract SerialNumber data from these fields.

        for cur_field in dict_srnum_cols:
            # cur_field = list(dict_srnum_cols.keys())[0]
            cur_qty = dict_srnum_cols[cur_field]

            df_data = df_temp[[cur_field, cur_qty, 'ContractNumber']].copy()
            df_data.columns = ['SerialNumberContract', 'Qty', 'ContractNumber']

            df_data.loc[:, 'SerialNumber'] = prep_data(
                df_data[['SerialNumberContract']], sep)

            df_data.loc[:, 'is_serialnum'] = df_data['SerialNumber'].apply(
                lambda x:
                    re.search(config_contract.pat_srnum1, str(x)) != None)

            # Expand Serial number
            ls_dfs = df_data.apply(lambda x: expand_srnum(
                x, config_contract.pat_srnum), axis=1).tolist()

            # Results
            df_ls_collapse = pd.concat(ls_dfs)
            df_ls_collapse['src'] = cur_field

            df_serialnum = pd.concat([df_serialnum, df_ls_collapse])

            # ENV_.logger.app_debug(f'{cur_field}: {df_serialnum.shape[0]}')
            del ls_dfs, df_data, df_ls_collapse

        df_serialnum = df_serialnum.reset_index(drop=True)

        ENV_.logger.app_success(_step)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_serialnum


def get_range_srum(df_temp_org):
    # df_temp_org = df_convert_rge[ls_cols].copy()
    _step = f"{' '*5}Identify Start-up"

    try:
        df_temp = df_temp_org.copy()
        del df_temp_org

        # Clean punctuation
        dict_char = {
            '(': "-",
            ' ': "-",
            '-&-': "-",
            '-&': "-",
            '&-': "-",
            '&': "-",
            'unit': '-',
            ',-': ',',
            '-,': ',',
            ':': '-'
        }

        for char in dict_char:
            # char = list(dict_char.keys())[0]
            sep = dict_char[char]

            #df_temp.loc[:, 'SerialNumber'] = (
            #    df_temp['SerialNumber'].str.replace(f"{char}", sep, regex=True))

            df_temp.loc[:, 'SerialNumber'] = df_temp['SerialNumber'].apply(
                lambda x: re.sub(f'{sep}+', sep, str(x)))

        # Prep Data
        df_temp = df_temp.rename(
            columns={'SerialNumber': 'SerialNumberOrg'})
        df_temp['SerialNumberOrg'] = df_temp[
            'SerialNumberOrg'].astype(str).str.lower()
        df_temp['SerialNumberOrg'] = df_temp[
            'SerialNumberOrg'].apply(
                lambda x: x.lstrip(punctuation).rstrip(punctuation))

        # Get Range
        df_expanded_srnum, df_could_not = SRNUM.get_serialnumber(
            df_temp.SerialNumberOrg, df_temp.Qty, 'contract')

        df_expanded_srnum['SerialNumberOrg'] = df_expanded_srnum[
            'SerialNumberOrg'].astype(str).str.lower()

        df_temp = df_temp.merge(
            df_expanded_srnum, how='left', on='SerialNumberOrg')

        ENV_.logger.app_success(_step)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_temp


def flag_serialnumber_wid_range(df_temp_org):
    """
    Identify if SerialNumber represents a range
    e.g xxx-xxx-1-5 represents 5 serial numbers.

    :param df_temp_org: DESCRIPTION
    :type df_temp_org: TYPE
    :raises Exception: DESCRIPTION
    :return: DESCRIPTION
    :rtype: TYPE

    """
    _step = f"{' '*5}Identify serialnumbers with range"

    try:
        df_temp = df_temp_org.copy()
        del df_temp_org

        # Based on quantity column from contracts
        df_temp.loc[:, 'flag_qty'] = (df_temp.Qty == 1)

        # Patterns without range
        df_temp.loc[:, 'flag_sr_type'] = df_temp.SerialNumber.apply(
            lambda x:
                re.search(
                    config_contract.pat_single_srnum, str(x)+" ") != None)

        # Quantity is zero and has only one separator
        df_temp.loc[:, 'flag_unknown_qty'] = df_temp.SerialNumber.apply(
            lambda x: len(re.findall(r'\-', x)) <= 2)

        df_temp.loc[:, 'flag_unknown_qty'] = (
            df_temp['flag_unknown_qty'] & (df_temp['Qty'] == 0))

        # Summarize Flags
        df_temp.loc[:, 'flag_single'] = (
            df_temp['flag_qty']
            | df_temp['flag_sr_type']
            | df_temp['flag_unknown_qty'])

        ENV_.logger.app_success(_step)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_temp['flag_single']


def filter_srnum(df_temp_org):
    _step = "Filter Serial Numbers"
    try:
        df_temp = df_temp_org.copy()
        del df_temp_org

        # Filter : Limit to DCPD products : STS / RPP / PDU
        df_temp.loc[:, 'Product'] = BL.idetify_product_fr_serial(
            df_temp['SerialNumber'])
        df_temp = df_temp[df_temp['Product'] != '']

        # Filter : Valid Serial Number
        df_temp['SerialNumber'] = df_temp['SerialNumber'].apply(
            lambda x: x.lstrip(punctuation).rstrip(punctuation))
        df_temp.loc[:, 'valid_sr'] = SRNUM.validate_srnum(
            df_temp['SerialNumber'])
        df_temp = df_temp.loc[df_temp['valid_sr'], :]

        # Filter : Mobile number (as they have simailar patterns to Serial Numbers)
        df_temp['flag_mob'] = df_temp.SerialNumber.apply(
            lambda x: re.search(config_contract.pat_mob, str(x)) != None)
        df_temp = df_temp[df_temp['flag_mob'] == False]

        df_temp = df_temp[
            ['SerialNumber', 'ContractNumber', 'SerialNumberContract',
             'Qty', 'src', 'Product']]
        df_temp = df_temp.reset_index(drop=True)
        ENV_.logger.app_debug(f"{_step} : SUCCEEDED", 3)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_temp


def expand_srnum(x, pat_srnum):
    """
    Expand SerialNumbers

    :param x: DESCRIPTION
    :type x: TYPE
    :param pat_srnum: DESCRIPTION
    :type pat_srnum: TYPE
    :raises Exception: DESCRIPTION
    :return: DESCRIPTION
    :rtype: TYPE

    """

    _step = f"expand_srnum"
    # x = df_data.loc[0, :]
    try:
        if not x['is_serialnum']:
            df_srnum = pd.DataFrame(data={
                'SerialNumber': np.nan,
                'ContractNumber': x['ContractNumber'],
                'SerialNumberContract': x['SerialNumberContract'],
                'Qty': x['Qty']}, index=[0]
            )
            return df_srnum

        sr_num = x['SerialNumber']

        ls_sr_num = []

        for cur_srnum_pat in pat_srnum:
            # cur_srnum_pat = pat_srnum[2]
            ls_sr_num_cur = re.findall(cur_srnum_pat, str(sr_num))

            if len(ls_sr_num_cur) > 0:
                # ENV_.logger.app_debug(
                #    f"SerialNum: {sr_num}\t pattern: {cur_srnum_pat}, "
                #    f"ideed sr_nums: {','.join(ls_sr_num_cur)}")
                ls_sr_num = ls_sr_num + ls_sr_num_cur
                sr_num = re.sub(cur_srnum_pat, "", sr_num)

            if len(sr_num) <= 2:
                break

        df_srnum = pd.DataFrame(data={'SerialNumber': ls_sr_num})
        df_srnum['ContractNumber'] = x['ContractNumber']
        df_srnum['SerialNumberContract'] = x['SerialNumberContract']
        df_srnum['Qty'] = x['Qty']

        #ENV_.logger.app_debug(f"{_step} : SUCCEEDED", 3)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_srnum


def prep_data(df_temp_org, sep):
    """
    Clean serial number fields before individual SerialNumber can be identified.

    :param df_temp_org: contracts data for PDI from SaleForce.
    :type df_temp_org: pandas DataFrame.
    :param sep: character used for separating two serial numbers
    :type sep: string
    :return: DESCRIPTION
    :rtype: TYPE

    """
    _step = "Pre-process SerialNumber fields"

    try:
        df_temp = df_temp_org.copy()
        df_temp.columns = ['SerialNumber']

        # Clean punctuation
        ls_char = ['\r',  '\n']  # '\.', , '\;', '\\'
        for char in ls_char:
            # char = ls_char[3]
            df_temp.loc[:, 'SerialNumber'] = (
                df_temp['SerialNumber'].str.replace(char, sep, regex=True))

            df_temp.loc[:, 'SerialNumber'] = df_temp['SerialNumber'].apply(
                lambda x: re.sub(f'{char}+', sep, x))

        # df_temp.loc[:, 'SerialNumber'] = (df_temp['SerialNumber'].str.replace('-t', sep + 't', regex=True))

        # Collapse multiple punctions
        df_temp.loc[:, 'SerialNumber'] = df_temp['SerialNumber'].apply(
            lambda x: re.sub(f'\{sep}+', sep, x))
        df_temp.loc[:, 'SerialNumber'] = df_temp['SerialNumber'] + sep

        ENV_.logger.app_debug(f"{_step} : SUCCEEDED", 3)

    except Exception as e:
        ENV_.logger.app_fail(_step,  f'{traceback.print_exc()}')
        raise Exception('f"{_step}: Failed') from e

    return df_temp['SerialNumber']


# %% ***** Data merge *****

def merge_contract_and_renewal(df_contract, df_renewal):

    _step = 'merge_contract_and_renewal'
    try:
        df_contract = df_contract.merge(
            df_renewal, on='Contract', how='inner')
        ENV_.logger.app_success(_step)
    except Exception as e:
        ENV_.logger.app_fail(_step, f"{traceback.print_exc()}")
        raise Exception from e

    return df_contract


def merge_contract_and_srnum(df_contract, df_contract_srnum):

    # Prep Contract Serial Number
    ls_cols = ['ContractNumber', 'Product', 'SerialNumber']
    df_contract_srnum = df_contract_srnum.loc[
        df_contract_srnum.flag_validinstall, ls_cols]

    # Prep Contract
    ls_cols = [
        'ContractNumber',
        'PDI_ContractType', 'Service_Plan',
        'Contract_Stage__c', 'StatusCode', 'Contract_Status_c',
        'Original_Sales_Order__c', 'PDI_Product_Family__c',
        'was_startedup', 'startup_date',
        'Start_Up_Completed_Date__c', 'Customer_Tentative_Start_Up_Date__c',
        'Scheduled_Start_Up_Date__c',
        'Warranty_Start_Date', 'Warranty_Expiration_Date',

        'Payment_Frequency', 'Start_date', 'BillingStreet',
        'BillingAddress', 'BillingCity', 'BillingState', 'BillingPostalCode',
        'BillingCountry',  #'Country__c',
        'StartupCustomer',
        'StartupAddress', 'StartupCity', 'StartupState', 'StartupPostalCode',
        'StartupCountry',
        'Contract', 'Service_Sales_Manager'
    ]
    df_contract = df_contract.loc[:, ls_cols]

    df_conract = df_contract_srnum.merge(
        df_contract, on='ContractNumber', how='inner')

    return df_conract


# %% *** Call ***


if __name__ == "__main__":
    main_contracts()

# %%
