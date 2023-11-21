#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
| Â© 2020 Eaton Corporation. All rights reserved.
|
| Author: PriyeshAgrawal@eaton.com
| Created On: 10/30/2021
"""

# %% ***** Setup Environment *****
# pylint: disable=E1101

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
#from azure.identity import ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeDirectoryClient
from io import BytesIO
from datetime import datetime
import logging
import json


class adlsFunc():
    """
    Process data on ADLS.

    Functions:
        - Initialize connection to ADLS
        - Read file from ADLS
        - Write file to ADLS
        - List files in given contaner algo with their timesatmp
        - Delete Files from ADLS
    """

    def read_credentials(self, ls_cred=[]):
        """
        Read the configurations related to ADLS and creates a dictionary.

        :return:
            Credentials as a dictionary
        """
        # Initialize
        logging.disable(logging.CRITICAL)

        if len(ls_cred) == 0:
            ls_cred = dict.fromkeys(
                ['ilead-adls-connection-string', 'ilead-storage-account'])
        else:
            ls_cred = dict.fromkeys(ls_cred)
        url_vault = "https://keyvaulta3caa815a4.vault.azure.net/"

        try :
            # Setup Environment
            credential = DefaultAzureCredential()
            secret_client = SecretClient(
                vault_url=url_vault, credential=credential)
        except Exception as e:
            print(f"Error: {str(e)}")
            

        # Query credentials ADLS Gen2 from Azure keys vaults
        dict_cred = {}
        for key in ls_cred:
            value = secret_client.get_secret(key)
            var_name = key.replace('-', '_')
            dict_cred[var_name] = value.value

        logging.disable(logging.NOTSET)

        return dict_cred

    def initialize_ADLS_storage_account(self, storage_account_name, client_id,
                                        client_secret, tenant_id):
        """
        Connect ADLS gen2 by using Azure Active Directory (Azure AD).

        Parameters
        ----------
        storage_account_name : string.
        client_id : string
        client_secret : string
        tenant_id: string

        Returns
        -------
        Initializes ADLS gen2 account

        """
        try:
            logging.disable(logging.CRITICAL)
            global service_client

            credential = ClientSecretCredential(
                tenant_id, client_id, client_secret)

            service_client = DataLakeServiceClient(
                account_url="{}://{}.dfs.core.windows.net".format(
                    "https", storage_account_name), credential=credential)

            logging.disable(logging.NOTSET)

        except Exception as e:
            return e

    def input_file_read(
            self, connection_string, container_name, file_name,
            directory_name='', sep=','):
        """
        Read files stored on ADLS Gen 2.

        Parameters
        ----------
        container_name : string.
        file_name : string. Name of the file to be read.
        directory_name : string, optional
          Sub-folder  within the container. Default is '' i.e file is stored in
          container.

        Returns
        -------
        Pandas Data Frame.
        If file does not exist or file is empty, Empty pandas data frame will
        be returned.
        """
        try:
            #logging.disable(logging.CRITICAL)
            logging.info('inside input file read')
            service_client = DataLakeServiceClient.from_connection_string(
                str(connection_string))

            container_client = service_client.get_file_system_client(
                file_system=container_name)
                
            if directory_name == '':
                logging.info('directory name empty')
                file_client = container_client.get_file_client(file_name)
            else:
                logging.info('directory name NOT empty')
                directory_client = container_client.get_directory_client(
                    directory_name)
                file_client = directory_client.get_file_client(file_name)

            download = file_client.download_file()
            downloaded_bytes = download.readall()
            out_df = pd.DataFrame()
            logging.info('before checking extension')
            if str(file_name.split('.')[-1]).lower() !='csv':
                try:
                    logging.info('inside parquet')
                    table = pq.read_table(BytesIO(downloaded_bytes))
                    out_df = table.to_pandas()
                    logging.info('after reading parquet')
                except Exception as parquet_error:
                    return parquet_error
            else:
                # If it's not a Parquet file, attempt to read as CSV or Excel
                try:
                    out_df = pd.read_csv(BytesIO(downloaded_bytes), sep=sep)
                    logging.info('inside csv')
                except Exception as csv_error:
                    return csv_error


            #logging.disable(logging.NOTSET)

            return out_df
        except Exception as e:
            return e

    def output_file_write(
            self, connection_string, dataset, output_container_name,
            output_file_name, output_directory_name=''):
        """
        Export data to blob storage.

        Parameters
        ----------
        dataset : Pandas Data Frame.
        output_container_name : string
            DESCRIPTION.
        output_file_name : final file
            DESCRIPTION.
        output_directory_name : string, optional
            DESCRIPTION. The default is ''.

        Returns
        -------
        Status and File name.
        """
        try:
            #logging.disable(logging.CRITICAL)
            logging.info('inside class_adlsfunc output write')
            dataset = dataset.replace('\n', '')
            logging.info(f'dataset after replace \n: {dataset}')
            service_client = DataLakeServiceClient.from_connection_string(
                str(connection_string))
            container_client = service_client.get_file_system_client(
                file_system=output_container_name)
            #data = bytes(dataset.to_csv(line_terminator='\n',index=False), encoding='utf-8')
            data = dataset.to_csv(index=False).replace('\r\n', '\n').encode('utf-8')
            logging.info(f'data after converting to bytes: {data}')
            final_file = output_file_name+".csv"

            if output_directory_name == '':
                logging.info('output directory name empty')
                output_file_client = container_client.create_file(final_file)
            else:
                directory_client = container_client.get_directory_client(
                    output_directory_name)
                directory_client.create_directory()
                output_file_client = directory_client.create_file(final_file)

            output_file_client.append_data(data, 0, len(data))
            output_file_client.flush_data(len(data))

            #logging.disable(logging.NOTSET)

            logging.info("Successfully exported the file (From ADLS block)")
            return f"Success! File created with name: {final_file}"
        except Exception as e:
            logging.info('within exception of write class_adls func')
            return e

    def list_ADLS_directory_contents(
            self, connection_string, container_name, directory_name=''):
        """
        List all files stored in ADLS Gen 2 container.

        Parameters
        ----------
        container_name : string.
        directory_name : string, optional, sub directory can be included
                         ex.: 'abc/xyz'

        Returns
        -------
        List of all files contained in the container with their last modified
        timestamp

        """
        try:
            #logging.disable(logging.CRITICAL)

            file_dict = {}
            service_client = DataLakeServiceClient.from_connection_string(
                str(connection_string))
            file_system_client = service_client.get_file_system_client(
                file_system=container_name)

            paths = file_system_client.get_paths(path=directory_name)

            for file in paths:
                file_dict[str(file.last_modified)] = str((file.name).split("/")[-1])
                #logging.info(f'file_dict in list files {file_dict}')

            if file_dict:
                file_modified, file_name = sorted(file_dict.items(), reverse=True)[0]

            else:
                file_modified = None
                file_name = None
            logging.info(f'file_name in list files {file_name}')
           # logging.disable(logging.NOTSET)

            return file_name

        except Exception as e:
            return e

    def delete_old_snapshot(
            self, connection_string, container_name, directory_name):
        """
        Delete old snapshots from ADLS directory.

        Parameters
        ----------
        container_name : string.
        directory_name : string, sub directory can be included ex.: 'abc/xyz/'

        Returns
        -------
        Files deleted with file path and last modified date

        """
        try:
            logging.disable(logging.CRITICAL)

            today_date = datetime.today().strftime('%Y-%m-%d')

            service_client = DataLakeServiceClient.from_connection_string(
                str(connection_string))

            file_system_client = service_client.get_file_system_client(
                file_system=container_name)

            find_file_client = file_system_client.get_paths(
                path=directory_name)

            for path in find_file_client:
                if str(path.last_modified) < today_date:
                    delete_file_client = file_system_client.get_file_client(
                        path=str(path.name))
                    delete_file_client._delete()

            logging.disable(logging.NOTSET)

            return "File deleted ", (path.name).split("/")[-1], str(path.last_modified)

        except Exception as e:
            return e

    def read_N_club_data(
            self, connection_string, container_name, directory_name='',
            sheet_name=''):
        """
        Read and club all the raw data files.

        Parameters
        ----------
        directory_in : path.
        ls_p_in : list of file names with raw data.

        Returns
        -------
        in_data : pandas data frame.
        """
        in_data = pd.DataFrame()
        try:
            logging.disable(logging.CRITICAL)

            file_dict = {}
            service_client = DataLakeServiceClient.from_connection_string(
                str(connection_string))
            file_system_client = service_client.get_file_system_client(
                file_system=container_name)

            paths = file_system_client.get_paths(path=directory_name)

            for file in paths:
                file_dict[str(file.last_modified)] = str(
                    (file.name).split("/")[-1])

                file_name = str((file.name).split("/")[-1])
                logging.info(f"On line 275 of ADLS, file name: {file_name}")
                # Read Data

                data = self.input_file_read(
                    connection_string, container_name, file_name, directory_name)

                # Concatenate Date
                if isinstance(data, pd.DataFrame):
                    in_data = pd.concat([in_data, data])
                    logging.info(
                        f"On ADLS block, size of in_data: {in_data.shape[0]},"
                        f" size of data: {data.shape[0]}")
                del data
            logging.disable(logging.NOTSET)
            return in_data

        except Exception as e:
            return e

    def column_name(
            self, connection_string, container_name, directory_name,
            file_name='model.json', table=''):
        """
        Read column names from model.json file.

        Parameters
        ----------
        connection_string : String
            Connection string for ADLS
        container_name : string
            Container name in which file is located
        directory_name : string
            Directory name in which file is located.
        file_name : string, optional
            Json file name from which column name need to be extracted.
            The default is 'model.json'.

        Returns
        -------
        col_list: list
            column name list.
        """
        col_list = []
        try:
            logging.disable(logging.CRITICAL)

            service_client = DataLakeServiceClient.from_connection_string(
                str(connection_string))
            container_client = service_client.get_file_system_client(
                file_system=container_name)
            directory_client = container_client.get_directory_client(
                directory_name)
            file_client = directory_client.get_file_client(file_name)
            download = file_client.download_file()
            downloaded_bytes = download.readall()
            json_data = json.loads(downloaded_bytes.decode('utf8'))
            if table != '':
                for i in range(0, len(json_data['entities'])):
                    if json_data['entities'][i]['name'] == table:
                        for col in range(0, len(json_data['entities'][i]['attributes'])):
                            col_name = json_data['entities'][i]['attributes'][col]['name']
                            col_list.append(col_name)
                        return col_list
            else:
                for col in range(0, len(json_data['entities'][0]['attributes'])):
                    col_name = json_data['entities'][0]['attributes'][col]['name']
                    col_list.append(col_name)
                return col_list

            logging.disable(logging.NOTSET)
        except Exception as e:
            return e
        

    



# %%




# %%
