# -*- coding: utf-8 -*-
"""
*********************************************************************
* @file json_creator.py
*
* @brief creates/modify json configuration file
* @details creating json configuration file involves:
    -reading the configuration data from the current excel file for
    dictionary format and Filters sheet
    -read data from config_env.py file
    -updating existing json file with new filters for existing/new database
    based on database/filter name
    -updating existing json file with new Format for existing/new database
    based on database name
    -method to read/write json file
    -expected datetime object needs to be converted to datetime manually
    since it will be stored as a string

* @copyright 2023 Eaton Corporation. All Rights Reserved.
* @note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used
without direct written permission from Eaton Corporation.
******************************************************************************
"""

import os
import re
import sys
import json
import inspect
import pandas as pd

import src.config_env as CONF
from src.class_help_logger import AppLogger

# initalising logger class
logger = AppLogger(app_name=__name__, level=CONF.LOG_LEVEL)


def update_config(db_config, n_file, db_name=None, db_filters=None,
                  dictionary_format=False, filters=False):
    """
    updates the filters/input_format from the xlsx configuration file
    based on database name or based on individual filter name incase of
    filters.

    :param db_config: readed json file
    :type db_config: dictionary
    :param n_file: name of the excel configuration in the "reference_files"
    directory
    :type n_file: string
    :param db_name: name of database to update
    :type db_name: string
    :param db_filters: list of database filters, defaults to None
    :type db_filters: list of strings, optional
    :param dictionary_format: True if the data in "Dictionary Format" sheet
    needs to be updated, defaults to False
    :type dictionary_format: boolean, optional
    :param filters: True if the data in "Filters" sheet needs to be updated,
    defaults to False
    :type filters: boolean, optional
    :return: Updated db_config
    :rtype: dictionary
    """
    try:
        # path to the excel configuration file
        if dictionary_format:
            if db_name is None:
                raise TypeError("Got None as db_name")
            db_config = update_format(n_file, db_name, db_config)

        if filters:
            if db_filters is not None:
                # updating individual filters
                db_config = (
                    update_filters_individual(db_config, db_filters, n_file)
                    )

            if db_name is not None:
                db_config = update_filters(n_file, db_name, db_config)

    except Exception as e:
        logger.app_fail("updating the json config ", e)
        sys.exit(1)

    return db_config


def update_format(n_file, db_name, db_config):
    """
    updates input format in the db_config corresponding to the database.
    If database is new entry creates a new slot

    :param n_file: name of excel configuraion file
    :type n_file: string
    :param db_name: database name
    :type db_name: string
    :param db_config: configuration dictionary
    :type db_config: dictionary
    :return: updated configruration dictionary
    :rtype: dictionary

    """
    try:

        sources = list(db_config['database_configs'])
        format_dict_from_xl = converting_to_string(
            config_format(db=db_name, n_file=n_file)
            )

        # to check whether the database is already present in db_config
        if db_name in sources:

            db_config['database_configs'][db_name]['input_format'] = \
                format_dict_from_xl
        else:
            # initialising a slot for the new database
            db_config['database_configs'][db_name] = {}
            db_config['database_configs'][db_name]['input_format'] = (
                format_dict_from_xl
                )
            db_config['database_configs'][db_name]['output_format'] = {""}
            db_config['database_configs'][db_name]['filters'] = {""}

    except Exception as e:
        logger.app_fail("updating format part", e)
        sys.exit(1)

    return db_config


def update_filters(n_file, db_name, db_config):
    '''
    update filters corresponding to the database,
    creates new slot if database is newly added

    :param n_file: name of excel configuraion file
    :type n_file: string
    :param db_name: database name
    :type db_name: string
    :param db_config: configuration dictionary
    :type db_config: dictionary
    :return: updated configruration dictionary with filter data
    :rtype: dictionary

    '''
    try:
        p_path = os.path.join(CONF.DIR_REF, n_file)
        # loading the "Filters" sheet
        df_data_filters = pd.read_excel(p_path,
                                        sheet_name="Filters",
                                        skiprows=3)
        ls_source_filter = (
            list(df_data_filters['data_source'].dropna().unique())
            )
        db_filter_dict = get_database_map(ls_source_filter)
        filters_list = db_filter_dict[db_name]
        if db_name in db_config['database_configs'].keys():

            db_config['database_configs'][db_name]['filters'] = (
                get_filters(filters_list, n_file)
                )
        else:
            db_config['database_configs'][db_name] = {}
            db_config['database_configs'][db_name]['filters'] = (
                get_filters(filters_list, n_file)
                )
            db_config['database_configs'][db_name]['output_format'] = {""}
            db_config['database_configs'][db_name]['input_format'] = {""}

    except Exception as e:
        logger.app_fail("updating filters part", e)
        sys.exit(1)
    return db_config


def update_filters_individual(conf, filters_list, n_file):
    """
    updates/adds individual filters in the filters_list to the conf dictionary
    initialises database if it's not defined

    :param conf: dictionary configuration
    :type conf: dictionary
    :param filters_list: list of filters to update
    :type filters_list: list of strings
    :param n_file: name of excel configuraion file
    :type n_file: string
    :return: updated dictionary configuration with filter data
    :rtype: dictionary

    """
    try:
        if not isinstance(filters_list, list):
            raise TypeError("The argument has to be list")

        for filter_ in filters_list:
            db = filter_.split("_")[0]
            if db not in conf['database_configs'].keys():
                conf['database_configs'][db] = {}
                conf['database_configs'][db]['filters'] = {}
                conf['database_configs'][db]['input_format'] = {}
                conf['database_configs'][db]['output_format'] = {}
                conf['database_configs'][db]['filters'][filter_] = (
                    config_filters(db=filter_, n_file=n_file)
                    )
            else:
                conf['database_configs'][db]['filters'][filter_] = (

                    config_filters(db=filter_, n_file=n_file)
                    )

    except Exception as e:
        logger.app_fail("updating filters individual part", e)
        sys.exit(1)
    return conf


def create_config_json(n_file, remove_list):
    """
    creates a dictionary from the excel and .py configuration file

    :param n_file: name of the excel configuration file
    :type n_file: string
    :param remove_list: list of variables to remove from .py configuration
    before converting to dict
    :type remove_list: list or None
    :return: configuration dictionary
    :rtype: dictionary

    """
    try:
        if not isinstance(n_file, str):
            msg = f"'{n_file}' has to be string, name of the .xlsx config file"
            raise TypeError(msg)
        if n_file.split(".")[-1] != 'xlsx':
            msg = f"'{n_file}' need to be .xlsx file"
            raise ValueError(msg)

        config = get_dict_from_xlsx(n_file)
        config = converting_to_string(config)
        dict_variable = get_dict_from_py(CONF, remove_list)
        config['configs'] = converting_to_string(dict_variable)

    except Exception as e:
        logger.app_fail("create logger function", e)
        sys.exit(1)

    return config


def get_dict_from_xlsx(n_file):
    """
    creates the dictionary from the configuration excel file
    in the format
    database_configs:
        all the databases used:
            filters : filter configuration from the "Filters" sheet
            input_format: configurations from the "Dictionary format" sheet
            output_format: output format configuration

    :param n_file: name of the xlsx config file in the "reference_file"
    directory
    :type n_file: string
    :return: configuration dictionary
    :rtype: dictionary

    """
    try:
        # path to the excel configuration file
        p_path = os.path.join(CONF.DIR_REF, n_file)
        # loading the "Dictionary Format" sheet
        df_data_format = pd.read_excel(p_path,
                                       sheet_name="Dictionary Format",
                                       skiprows=3)
        # loading the "Filters" sheet
        df_data_filters = pd.read_excel(p_path,
                                        sheet_name="Filters",
                                        skiprows=3)

        # getting the unique sources
        ls_source_format = (
            list(df_data_format['data_source'].dropna().unique())
            )
        # finding the unique filter names
        ls_source_filter = (
            list(df_data_filters['data_source'].dropna().unique())
            )

        # initialising dictionaries
        config = {}
        database_dict = {}

        # getting the "database: list of filter" dictionary for each databases
        db_filter_dict = get_database_map(ls_source_filter)

        # going through each data base got from "Filters" sheet
        for data_base in db_filter_dict:
            database_dict[data_base] = {}
            # attaching the filters dict
            filters_list = db_filter_dict[data_base]
            database_dict[data_base]['filters'] = (
                get_filters(filters_list, n_file)
                )
            # attaching the input config format
            database_dict[data_base]['input_format'] = (
                get_format(data_base, n_file, ls_source_format)
                )
            # removing the database from the format sources
            print(data_base)
            if data_base in ls_source_format:
                ls_source_format.remove(data_base)

            # place holder for output_format
            database_dict[data_base]['output_format'] = {" "}

        for data_base in ls_source_format:
            # iterating throught all the database which is only unique to
            # "Dictionary Format" sheet
            database_dict[data_base] = {}
            database_dict[data_base]['filters'] = {" "}
            database_dict[data_base]['input_format'] = (
                get_format(data_base, n_file, ls_source_format)
                )
            database_dict[data_base]['output_format'] = {" "}

        config['database_configs'] = database_dict

    except Exception as e:
        logger.app_fail("getting dict from xlsx file", e)
        sys.exit(1)
    return config


def get_filters(filters_list, n_file):
    """
    binding together all the filters corresponding to a database
    as a single dictionary with key as filter name

    :param filters_list: list of filters
    :type filters_list: list
    :param n_file: name of the xlsx config file in the "reference_file"
    directory
    :type n_file: string
    :return: dictionary of filters with key as filter name
    :rtype: dictionary

    """
    try:
        filters = {}
        for filters_ in filters_list:
            filters[filters_] = (
                config_filters(db=filters_, n_file=n_file)
                )

    except Exception as e:
        logger.app_fail("getting filters from filter list", e)
        sys.exit(1)

    return filters


def get_format(data_base, n_file, ls_source_format):
    """
    getting input format corresponding to database name if it is present in
    ls_source_format else initialising it


    :param data_base: database name
    :type data_base: string
    :param n_file: name of the xlsx config file in the "reference_file"
    directory
    :type n_file: string
    :param ls_source_format: list of sources present in "Format" sheet
    :type ls_source_format: list
    :return: dictionary of read format
    :rtype: dictionary

    """
    try:
        if not isinstance(data_base, str):
            raise TypeError("The database name has to be string")
        if not isinstance(ls_source_format, list):
            raise TypeError('ls_source_format has to be a list ')
        if data_base in ls_source_format:
            format_ = config_format(db=data_base, n_file=n_file)

        else:
            format_ = {" "}

    except Exception as e:
        logger.app_fail("getting format", e)
        sys.exit(1)
    return format_


def converting_to_string(dict_):
    """
    Converting each value of the dictionary to string if it is not of
    datatype int, float, bool, list or str
    :param dict_: dictionary whose value's data type needs to be changed to
    string
    :type dict_: dictionary
    :return: dictionary with string as the datatype of it's values
    :rtype: dictionary

    """
    try:
        for key, value in dict_.items():
            if isinstance(value, dict):
                converting_to_string(value)
            else:
                if not isinstance(value, (int, float, bool, list, str)):
                    dict_[key] = str(value)

    except Exception as e:
        logger.app_fail("converting dictionary values to string", e)
        sys.exit(1)
    return dict_


def converting_to_date_obj(dict_):
    """
    converting to pandas datetime object if is convertable
    :param dict_: config file
    :type dict_: dictionary
    :return: config file with converted date object
    :rtype: dictionary

    """

    try:
        for key, value in dict_.items():
            if isinstance(value, dict):
                converting_to_date_obj(value)
            else:
                # checking whether if the string is of date format
                if  re.fullmatch("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
                                 str(value)
                                 ) != None:
                    dict_[key] = pd.to_datetime(value)

    except Exception as e:
        logger.app_fail("converting dictionary string values to date format", e)
        sys.exit(1)
    return dict_


def get_database_map(lst):
    """
    splitting the string based on "_" and getting a dictionary where
    the key is the name of the database and the value is the list of
    filters used in the database.

    :param lst: list of filters, with database_filtername format
    :type lst: list
    :return: dictionary which maps to database to its filters
    :rtype: dictionary

    """
    try:
        if not isinstance(lst, list):
            raise TypeError("The input has to be a list")
        map_ = {}
        for word in lst:
            first_word = word.split("_", 1)[0]
            if map_.get(first_word) is None:
                map_[first_word] = map_.get(first_word, []) + [word]
            else:
                value = map_[first_word]
                if word not in value:
                    map_[first_word].append(word)

    except Exception as e:
        logger.app_fail("getting database map from list of filters", e)
        sys.exit(1)

    return map_


def get_dict_from_py(CONF_, remove_list=None):
    """
    getting the variable and its value defined in the configenv.py file
except modules, dunders, variables in the list ['ENV', 'env', 'all_cols_path',
'basepath'] and variables in remove_list
    :param CONF_: imported configenv.py as CONF
    :type CONF_: module
    :param remove_list: list of variables to remove, defaults to []
    :type remove_list: list, optional
    :return: dictionary of variables and its values
    :rtype: dictionary

    """
    try:
        if not inspect.ismodule(CONF_):
            message = f"'{CONF_}' is not a module"
            raise TypeError(message)

        if remove_list is None:
            remove_list = []
        else:
            if not isinstance(remove_list, list):
                message = f"'{remove_list}' has to be either None or list"
                raise TypeError(message)
        members = inspect.getmembers(CONF_)

        variables = [(name, value) for name, value in members
                     if not callable(value)
                     and not name.startswith("__")
                     and not inspect.ismodule(value)
                     and not name.startswith("DIR")
                     and name not in ['ENV', 'env', 'all_cols_path', 'basepath']
                     and name not in remove_list]
        dict_variable = dict(variables)

    except Exception as e:
        logger.app_fail("getting dict from .py file", e)
        sys.exit(1)

    return dict_variable


def write_to_json(dict_, dir_name, encoding="utf-8"):
    """
    writing the dict_ to the location dir_name if write is True
    :param dict_: dictionary to save as a json
    :type dict_: dictionary
    :param dir_name: path of the json file
    :type dir_name: string
    :param encoding: encoding, defaults to "utf-8"
    :type encoding: string, optional
    :param write: whether to save or not, defaults to True
    :type write: bool, optional
    :return: None
    :rtype: None

    """
    try:
        # converting to format which
        dict_ = converting_to_string(dict_)

        with open(dir_name, "w", encoding=encoding) as outfile:
            json.dump(dict_, outfile, indent=4)

    except Exception as e:
        logger.app_fail("writing a json file", e)
        sys.exit(1)


def read_json(path, encoding="utf-8"):
    """
    reads json file from the location path

    :param path: path to the json configuration file
    :type path: string
    :param encoding: encoding, defaults to "utf-8"
    :type encoding: string, optional
    :return: json file as a dictionary
    :rtype: dictionary
    """
    try:
        with open(path, "r", encoding=encoding) as file:
            config = json.load(file)
        config = converting_to_date_obj(config)

    except Exception as exp:
        logger.app_fail("reading json configuration file", exp)
        sys.exit(1)

    return config


def config_filters(db, n_file='config_filters.xlsx', n_sheet='Filters'):
    """
    Generate config setting based on filters file.

    Parameters
    ----------
    n_file : string, optional
        Source specific filters.
        The default is 'config_filters.xlsx'.
    n_sheet : string, optional
        Sheet name of filters for source.
        The default is 'Filters Oracle Skynet'.

    Returns
    -------
    dict_out : dictionary. Dictionry set up is as follows:
        {field_name : {
            'action' : drop or keep,
            'type': text or numeric or date,
            filters optional
            }}
    if type = text:
        text_match_exact,
        text_match_pattern, text_match_pattern_negative,
        text_minimum_length, text_maximum_length
    if type = date:
        date_min, date_max
    if type = numeric:
        numeric_min, numeric_max, numeric_list
    """
    try:
        logger.app_debug('Read config filters')
        p_path = os.path.join(CONF.DIR_REF, n_file)
        df_data = pd.read_excel(p_path,
                                sheet_name=n_sheet,
                                skiprows=3)
        df_data = df_data[df_data['data_source'] == db]
        if len(df_data) == 0:
            message = f"could not find '{db}' in the" \
                f"'{n_file}'file in '{n_sheet}' sheet"
            raise ValueError(message)

        dict_out = {}

        n_rows = []
        for row in df_data.index:
            # row = df_data.index[0]
            col_name = df_data.field_name[row]
            count = sum(
                1 * (col_name == pd.Series(n_rows[:row]))) if row > 1 else 0
            if count == 0:
                n_rows.append(df_data.field_name[row])
            else:
                n_rows.append(f"{col_name} ({count})")

        df_data['r_index'] = n_rows
        df_data = df_data.set_index(df_data['r_index'])

        # Ensure there is no mismatch in filters
        for col in df_data.index:
            # col = df_data.index[1]
            logger.app_debug(col, 1)
            d_type = df_data.type[col]
            ls_cols = [(col) for col in df_data.columns if (d_type in col)]

            ls_cols = ['report', 'type'] + ls_cols
            dict_temp = df_data.loc[col, ls_cols]
            dict_temp = dict_temp.dropna()

            if col in list(dict_out.keys()):
                dict_out[col+'_'] = dict_temp.T.to_dict()
            else:
                dict_out[col] = dict_temp.T.to_dict()

            del dict_temp

    except Exception as e:
        logger.app_fail("config_filters function", e)
        sys.exit(1)

    return dict_out


def config_format(db, n_file='config_filters.xlsx', n_sheet='Dictionary Format'):
    """
    Generate config setting based on filters file.

    :param db: Database name as it appears in config file, defaults to 'InstallBase'
    :type db: string, optional
    :param n_file: Config file name with filter and format details, defaults to 'config_filters.xlsx'
    :type n_file: string, optional
    :param n_sheet: sheet name containing format details, defaults to 'Dictionary Format'
    :type n_sheet: string, optional
    :return: dictionary column renames and data types. Format for
    'dict_col_dtype' is given below:
        Output_Column_Name : {
        0. Original_Column_Name, 1. Data_Type, 2. bool_drop_na,
        3. input_date_type(optional), 4. output_date_type(optional))}
    :rtype: dictionary.

    """
    try:
        p_path = os.path.join(CONF.DIR_REF, n_file)
        df_data = pd.read_excel(
            p_path, sheet_name=n_sheet, skiprows=3)

        ls_cols = [
            'actual_datasoure_name', 'data_type', 'is_nullable',
            'output_format', 'input_date_format']

        df_data = df_data.set_index(df_data['ui_field_name'])
        df_data = df_data.loc[df_data['data_source'] == db, ls_cols]

        if len(df_data) == 0:
            message = f"could not find '{db}' in the" \
                f"'{n_file}' file in '{n_sheet}' sheet"
            raise ValueError(message)

        df_data = df_data.fillna('')

        dict_out = df_data.T.to_dict()

    except Exception as e:
        logger.app_fail("config_format", e)
        sys.exit(1)

    return dict_out
