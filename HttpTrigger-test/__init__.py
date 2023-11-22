import logging
import json
import os
import azure.functions as func
import numpy as np
from utils.io import IO
import pandas as pd
from utils import Format
#from utils.dcpd.class_installbase import InstallBase

#ib = InstallBase()
obj_format = Format()
IO = IO()





def read_ref_data(mode,config, ref_ac_manager=None):
    """
    Read reference data.

    :raises Exception: if read reference data failed
    :return: reference data for identifying strategic customer.
    :rtype: pandas dataframe

    """

    try:
        logging.info('inside read reference data')
        # Read: Reference Data
        _step = "Read reference data"

        if ref_ac_manager is not None:
            ref_ac_manager = ref_ac_manager
            logging.info('line:135-read ac manager')
        else:
            logging.info('line:137-read ac manager')
            logging.info(f'mode: {mode}')
            adls_config = config['file']['Reference']['adls_credentials']
            adls_dir= config['file']['Reference']['customer']
            logging.info(f'adls_config: {adls_config}')
            logging.info(f'adls_dir: {adls_dir}')
            ref_ac_manager = IO.read_csv(
                mode,
                {'file_dir': config['file']['dir_ref'],
                'file_name': config['file']['Reference']['customer'],
                'adls_config': config['file']['Reference']['adls_credentials'],
                'adls_dir': config['file']['Reference']['customer'],
                'sep': '\t'
                }
            )
        logging.info(f'read ac manager {ref_ac_manager.head()}')

        if ref_ac_manager.columns[0] != "Display":
            ref_ac_manager = ref_ac_manager.reset_index()
        logging.info(f'ref_ac_anager: 56')
        # Post Process
        ref_ac_manager.columns = ref_ac_manager.loc[0, :]
        ref_ac_manager = ref_ac_manager.drop(0)

        ref_ac_manager = ref_ac_manager[pd.notna(
            ref_ac_manager.MatchType_01)]
        ref_ac_manager = ref_ac_manager.fillna('')
    # ref_ac_manager = ref_ac_manager.fillna('')

        # Format data
        for col in ref_ac_manager.columns:
            ref_ac_manager.loc[:, col] = ref_ac_manager[
                col].astype(str).str.lower()

        #logger.app_success(_step)

        return ref_ac_manager

    except Exception as e:
        #logger.app_fail(_step, f"{traceback.print_exc()}")
        raise e


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Starting function app")
    config_dir = os.path.join(os.path.dirname(__file__), "../config")
    config_file = os.path.join(config_dir, "config_dcpd.json")

    # Read the configuration file
    with open(config_file, 'r') as config_file:
        config = json.load(config_file)
    # logging.info(f"Printing Config:\n")
    # logging.info(config)
    mode = config.get("conf.env", "azure-adls")

    try:
        # Read SerialNumber data
        logging.info('indside main_customer')
        _step = 'Read data : reference'
        ref_df = read_ref_data(mode,config)
        #logger.app_success(_step)
        logging.info(f'ref_df :{ref_df.head()}')
        


        
    except Exception as excp:
        logging.error('Exception occurred: %s', str(excp))
        return func.HttpResponse("Internal Server Error", status_code=500)

