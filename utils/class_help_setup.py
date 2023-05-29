
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup *****

from src.filter_data import ConfigureFilts
from src.logger import AppLogger
import os
import sys
import pandas as pd
from pathlib import Path

import logging
import traceback
import json

sys.path.append(str(Path(__file__).parent.parent))

# %% ***** Setup *****


class SetupEnvironment:

    def __init__(self, business, dict_json_file):

        self.business = business

        # Identify Environment
        self.CONF = self.indentify_env()

        # Read configuration
        for t_config in dict_json_file:
            # t_config  = list(dict_json_file.keys())[0]
            file_name = dict_json_file[t_config]
            json_config = self.read_config(file_name)
            self.CONF[t_config] = json_config

            del json_config

        # Initioalize logger
        self.set_logger(business)

        # Initialize Filters
        # self.CONF['system']['log_level']
        self.initialie_filters()

    # *** Environment ***

    def indentify_env(self):
        env = os.environ
        if 'WEBSITE_SITE_NAME' in env.keys():
            self.ENV = 'cloud'
        else:
            self.ENV = 'local'

        # Identify Directories
        from src.get_path import basepath

        if 'src' in str(basepath):
            DIR_PROJECT = str(basepath.parent)
        else:
            DIR_PROJECT = str(basepath)

        if self.ENV == 'local':
            os.chdir(DIR_PROJECT)

        DIR_PROJECT = '.'
        CONF = {}
        CONF['DIR_SRC'] = os.path.join(DIR_PROJECT, 'src')
        CONF['DIR_DATA'] = os.path.join(DIR_PROJECT,  'data')
        CONF['DIR_TEST'] = os.path.join(DIR_PROJECT,  'test')
        CONF['DIR_REF'] = os.path.join(DIR_PROJECT,  'references')
        CONF['DIR_RESULT'] = os.path.join(DIR_PROJECT,  'results')

        return CONF

    # *** Read config JSON ***

    def read_config(self, file_name):

        try:
            _step = 'Read JSON data'

            json_file_path = os.path.join(self.CONF['DIR_REF'], file_name)
            with open(json_file_path, "r") as jsonfile:
                json_config = json.load(jsonfile)

            logging.info(
                f'INFO:{self.business}: STEP 0 : {_step} : SUCCEEDED')
        except Exception as e:
            logging.info(
                f'ERROR:{self.business}:Failed in {_step} \n{e}')
            sys.exit()

        return json_config

    # *** Initialze Logger ***

    def set_logger(self, business):

        try:
            _step = 'Initialize logger'
            level = self.CONF['system']['log_level']
            self.logger = AppLogger(self.business, level=level)
            self.logger.app_success(_step)
        except Exception as e:
            logging.info(
                f'ERROR:{self.business}:Failed in initializing logger \n{e}')
            sys.exit()
        return 'successfull !'

    # *** Initialize Filters ***

    def initialie_filters(self):
        _step = 'Initialize filters'
        try:
            self.filters_ = ConfigureFilts(self.logger)
            self.logger.app_success(_step)
        except:
            self.logger.app_fail(_step, f"{traceback.print_exc()}")
            sys.exit()
        return 'successfull !'

    def export_data(self, df_data, db, type_="output"):
        """

        :param df_data: DESCRIPTION
        :type df_data: TYPE
        :param db: DESCRIPTION
        :type db: TYPE
        :param type_: DESCRIPTION, defaults to "output"
        :type type_: TYPE, optional
        :raises Exception: DESCRIPTION
        :return: DESCRIPTION
        :rtype: TYPE

        """
        _step = 'Export Data'

        try:
            if self.ENV == "local":
                #n_file = self.CONF['files']['Processed'][db][type_]
                n_file = self.CONF['files'][type_][db]['local_file']

                df_data.to_csv(
                    os.path.join(self.CONF['DIR_RESULT'], n_file), index=False)
            else:
                print('do something!')
            self.logger.app_debug(_step, 1)
        except Exception as e:
            self.logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception(e)

        return 'successfull !'

    def read_data(self, db, type_='data', sep=','):

        # Read Data
        _step = f'Read {db} data'
        try:
            # Read Data
            if self.ENV == 'local':
                if type_ == 'data':
                    file_name = self.CONF['files']['Raw'][db]['local_file']
                    file_path = self.CONF['DIR_DATA']
                elif type_ == 'processed':
                    file_name = self.CONF['files']['Processed'][db]['local_file']
                    file_path = self.CONF['DIR_RESULT']
                elif type_ == 'reference':
                    file_name = self.CONF['files']['Reference'][db]
                    file_path = self.CONF['DIR_REF']
                p_data = os.path.join(file_path, file_name)
                df_data = pd.read_csv(p_data, sep=sep)
            else:
                df_data = None
            self.logger.app_success(_step)
        except Exception as e:
            self.logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception(e)

        # Format Data
        if type_ != 'data':
            return df_data

        _step = 'Format data'
        try:
            dict_format = self.CONF['database'][db]['Dictionary Format']
        except Exception as e:
            self.logger.app_debug(f'Format for {db} not in config')
            return df_data

        try:
            df_data = self.filters_.format_data(df_data, dict_format)
            self.logger.app_success(_step)
        except Exception as e:
            self.logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception(e)

        return df_data

# %%
