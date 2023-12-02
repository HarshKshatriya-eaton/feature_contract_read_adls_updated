
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% *** Setup Environment ***

from utils.class_iLead_contact import ilead_contact
import logging
import utils.io_adopter.local as io_local
import pandas as pd
from datetime import datetime
from utils.io_adopter.class_adlsFunc import adlsFunc
#from azure.storage.filedatalake import DataLakeServiceClient
from utils import AppLogger
import re
logger = AppLogger(__name__)

# %% Define class
io_adls = adlsFunc()
class IO():

    @staticmethod
    def read_csv_adls(config) -> pd.DataFrame:
        connection_string_key = config['adls_config']['connection_string']
        storage_account_name_key = config['adls_config']['storage_account_name']
        try:
            logging.info('going to read credentials')
            credentials=io_adls.read_credentials(ls_cred=[connection_string_key,storage_account_name_key])
            logger.app_info(f'Mode {credentials} is fetched')
            formatted_connection_string_key = connection_string_key.replace('-', '_')
            # Accessing values from the credentials dictionary
            connection_string = credentials.get(formatted_connection_string_key)

            container_name=config['adls_dir']['container_name']
            directory_name= config['adls_dir']['directory_name']
            if 'file_name' in config['adls_dir'] and config['adls_dir']['file_name'] != "":
                file_name = config['adls_dir']['file_name']
                logger.app_info(f"file name In adls_dir: {file_name}")
            else:
                file_name = io_adls.list_ADLS_directory_contents(connection_string, container_name, directory_name)
                logger.app_info(f"file name from list: {file_name}")
            logger.app_info("Function is starting.")
            
            logger.app_info(f'connection String: {connection_string}\n, Container name: {container_name}\n, file name: {file_name}\n,  directory name:{directory_name}')
            result= io_adls.input_file_read(connection_string, container_name, file_name, directory_name=directory_name, sep=',')
            logger.app_info(f"Type of result: {result}")
            
            return result
           
        except Exception as e:
            raise e
    @staticmethod    
    def write_csv_adls(config,dataset):
        logger.app_info('inside write_csv_adls')
        connection_string_key = config['adls_config']['connection_string']
        storage_account_name_key = config['adls_config']['storage_account_name']
        try:
            credentials=io_adls.read_credentials(ls_cred=[connection_string_key,storage_account_name_key])
            logger.app_info(f'Credentials of write {credentials} is fetched')
            formatted_connection_string_key = connection_string_key.replace('-', '_')
            # Accessing values from the credentials dictionary
            connection_string = credentials.get(formatted_connection_string_key)
            output_container_name=config['adls_dir']['container_name']
            output_directory_name= config['adls_dir']['directory_name']

            if 'file_name' in config['adls_dir'] and config['adls_dir']['file_name'] != "":
                file_name= config['adls_dir']['file_name']
                if file_name.endswith(".csv"):
                    #file_name = file_name[:-4]+'dev'
                    file_name = file_name[:-4]
            else:
                 file_name = io_adls.list_ADLS_directory_contents(connection_string, output_container_name, output_directory_name)

            logger.app_info(f'file name: {file_name}')
            #timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            #output_file_name = f"{file_name}_{timestamp}"
            output_file_name = f"{file_name}"
            #dataset.to_csv(output_file_name, index=False)
            logger.app_info(f"Type of dataset: {dataset}")
            logger.app_info(f'connection String: {connection_string}\n, Container name: {output_container_name}\n, file name: {output_file_name}\n,  directory name:{output_directory_name}')
            result= io_adls.output_file_write(connection_string, dataset, output_container_name,output_file_name, output_directory_name)
            return result
        except Exception as e:
            return e

    # *** CSV ***
    @staticmethod
    def read_csv(mode, config) -> pd.DataFrame:

        if mode == 'local':
            return io_local.read_csv_local(config)
        elif mode == 'azure-adls':
            logger.app_info(f'Mode {mode} is implemented')
            #logger.app_info(f'Mode {config} is fetched')
            return IO.read_csv_adls(config)
        else:
            logger.app_info(f'Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')

    @staticmethod
    def write_csv(mode, config, data):
        logger.app_info('inside write csv function IO module')

        if mode == 'local':
            return io_local.write_csv_local(config, data)
        elif mode == 'azure-adls':
            logger.app_info(f'data {data} is passed to io')
            logger.app_info(f'config {config} is fetched')
            return IO.write_csv_adls(config,data)
        else:
            logger.app_info(f'Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')

    # *** JSON ***
    @staticmethod
    def read_json(mode, config):

        if mode == 'local':
            return io_local.read_json_local(config)
        elif mode == 'azure-adls':
            return io_local.read_json_local(config)
        else:
            logger.app_info(f'Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')

    
#%%
