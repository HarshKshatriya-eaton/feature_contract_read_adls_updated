
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% *** Setup Environment ***

import utils.io_adopter.local as io_local
import pandas as pd
from datetime import datetime
import utils.io_adopter.class_adlsFunc as io_adls
from azure.storage.filedatalake import DataLakeServiceClient
from utils import AppLogger
logger = AppLogger(__name__)

# %% Define class

class IO():


    def read_csv_adls(config) -> pd.DataFrame:
        connection_string_key = config['adls_config']['connection_string']
        storage_account_name_key = config['adls_config']['storage_account_name']
        try:
            credentials=io_adls.read_credentials(ls_cred=[connection_string_key,storage_account_name_key])
            connection_string = credentials['ilead-adls-connection-string']
            storage_account_name = credentials['ilead-storage-account']
            container_name=config['adls_dir']['container_name']
            directory_name= config['adls_dir']['directory_name']
            file_name = io_adls.get_latest_file_in_directory(storage_account_name,directory_name)

            return io_adls.input_file_read(
            connection_string, container_name, file_name,
            directory_name='', sheet_name='', sep=',')
           
        except Exception as e:
            raise e
        
    def write_csv_adls(config,dataset):
        connection_string_key = config['adls_config']['connection_string']
        storage_account_name_key = config['adls_config']['storage_account_name']
        try:
            credentials=io_adls.read_credentials(ls_cred=[connection_string_key,storage_account_name_key])
            connection_string = credentials['ilead-adls-connection-string']
            storage_account_name = credentials['ilead-storage-account']
            output_container_name=config['adls_dir']['container_name']
            output_directory_name= config['adls_dir']['directory_name']
            file_name= config['adls_dir']['file_name']
            timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            output_file_name = f"{file_name}_{timestamp}"
            dataset.to_csv(output_file_name, index=False)
            result= io_adls.output_file_write(
            connection_string, dataset, output_container_name,
            output_file_name, output_directory_name)
           
        except Exception as e:
            return e

    # *** CSV ***
    @staticmethod
    def read_csv(mode, config) -> pd.DataFrame:

        if mode == 'local':
            return io_local.read_csv_local(config)
        elif mode == 'azure-adls':
            return IO.read_csv_adls(config)
        else:
            logger.app_info(f'Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')

    @staticmethod
    def write_csv(mode, config, data):

        if mode == 'local':
            return io_local.write_csv_local(config, data)
        elif mode == 'azure-adls':
            return IO.write_csv_adls(config,data)
        else:
            logger.app_info(f'Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')

    # *** JSON ***
    @staticmethod
    def read_json(mode, config):

        if mode == 'local':
            return io_local.read_json_local(config)
        else:
            logger.app_info(f'Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')






#%%
