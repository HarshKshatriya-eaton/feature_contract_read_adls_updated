
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
import utils.io_adopter.class_adlsFunc as io_adls
from azure.storage.filedatalake import DataLakeServiceClient
from utils import AppLogger
logger = AppLogger(__name__)

# %% Define class

class IO():

    # *** CSV ***
    @staticmethod
    def read_csv(mode, config) -> pd.DataFrame:

        if mode == 'local':
            return io_local.read_csv_local(config)
        else:
            logger.app_info(f'Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')

    @staticmethod
    def write_csv(mode, config, data):

        if mode == 'local':
            return io_local.write_csv_local(config, data)
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


    def read_csv_adls(config) -> pd.DataFrame:
        connection_string = config['adls']['connection_string']
        container_name = config['adls']['container_name']
        file_name = config['adls']['file_path']

        try:
            service_client = DataLakeServiceClient.from_connection_string(connection_string)
            container_client = service_client.get_container_client(container_name)
            file_client = container_client.get_file_client(file_name)

            with file_client.get_file_client() as file:
                 file_contents = file.read_file()
            # Parse file_contents as a pandas DataFrame or perform other data processing

            return pd.DataFrame()  # Return the DataFrame
        except Exception as e:
        # Handle any exceptions or errors
        raise e
    
    def write_csv_adls(config,dataset):
        connection_string = 
        self, connection_string, dataset, output_container_name,
            output_file_name, output_directory_name='

#%%
