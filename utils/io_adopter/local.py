"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

import pandas as pd
import os
import traceback
import json

from utils.logger import AppLogger

logger = AppLogger(__name__)


#  *** JSON ***

def read_json_local(config):
    _step = f'Read json : {config}'
    try:
        file_name = config['file_name']
        file_dir = config['file_dir']

    except Exception as e:
        logger.app_fail("Required config not provided", 1)
        raise ValueError from e

    try:
        file_path = os.path.join(file_dir, file_name)

        with open(file_path, "r") as jsonfile:
            json_config = json.load(jsonfile)

        logger.app_debug(f"{_step}: SUCCEED", 1)
        return json_config

    except Exception as e:
        logger.app_fail(_step, f"{traceback.print_exc()}")
        raise Exception from e


#  *** CSV ***
def read_csv_local(config):
    """
    Method to read csv file from local machine
    @param config: config contains location of the file, filename and encoding
    @return: pandas dataframe for the csv file
    """
    _step = f'Read csv : {config}'
    try:
        file_name = config['file_name']
        file_dir = config['file_dir']
        sep = ',' if 'sep' not in config else config['sep']
        encoding = 'utf-8' if 'encoding' not in config else config['encoding']

    except Exception as e:
        logger.app_fail("Required config not provided", 1)
        raise ValueError from e

    try:
        file_path = os.path.join(file_dir, file_name)
        data = pd.read_csv(file_path, sep=sep, encoding=encoding)

        logger.app_debug(f"{_step}: SUCCEED", 1)
        return data

    except Exception as e:
        logger.app_fail(_step, f"{traceback.print_exc()}")
        raise Exception from e


def write_csv_local(config, data):
    _step = f'Write csv : {config}'

    try:
        file_name = config['file_name']
        file_dir = config['file_dir']
    except Exception as e:
        logger.app_fail("Required config not provided", 1)
        raise ValueError from e

    try:
        file_path = os.path.join(file_dir, file_name)
        data.to_csv(file_path, index=False)

        logger.app_debug(f"{_step}: SUCCEED", 1)
        return 'successful !'

    except Exception as e:
        logger.app_fail(_step, f"{traceback.print_exc()}")
        raise Exception from e

# %%
