# -*- coding: utf-8 -*-
"""@file



@brief Convert range of SerialNumber to unique SerialNumber


@details


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# Import system path
import sys

sys.path.append(".")

# Initialize class instance
import pandas as pd
import pytest
from utils.dcpd.class_serial_number import SerialNumber

sr_num_class = SerialNumber()


# Pytest execution command
# !pytest ./test/test_class_serial_num.py
# !pytest --cov
# !pytest --cov=.\src --cov-report html:.\coverage\ .\test\
# !pytest --cov=.\src\class_ProSQL.py --cov-report html:.\coverage\ .\test\

# Coverage code
# !coverage run -m pytest  -v -s
# !coverage report -m

# from pathlib import Path
# import sys
# # sys.path.append(str(Path(__file__).parent.parent))

# %% Validating unit testcases for prep_srnum function

@pytest.mark.parametrize(
    "df_input",
    [
        (['140-0063B&E']),
        (None),
        (pd.DataFrame(data={'SerialNumOrg': ['col1']})),
        (['180-0557a,b'])
    ],
)
def test_prep_srnum_input_validate(df_input):
    '''
    Validates the input dataframe parameters.

    Parameters
    ----------
    df_input : Pandas Dataframe
        DESCRIPTION. Serial number is passed to the function.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.prep_srnum(df_input)


def test_prep_srnum_empty_data():
    '''
    Validates output for empty dataframes

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.prep_srnum(pd.DataFrame())
    assert info.type != ValueError


def test_prep_srnum_ideal_data():
    '''
    Validates ideal data passed to function.

    Returns
    -------
    None.

    '''
    df_input = pd.DataFrame(data={'SerialNumberOrg':
                                      [' STS20134-299',
                                       '#ONYMG-1949-3784 ']
                                  })

    actual_op = sr_num_class.prep_srnum(df_input)
    exp_op = ['STS20134-299',
              'ONYMG-1949-3784']
    assert all([a == b for a, b in zip(actual_op, exp_op)])
    # assert actual_op == exp_op  # Both methods can be used.


# %% Py tests for Validate get_serialnumber function


@pytest.mark.parametrize(
    "ar_serialnum, ar_installsize, ar_key_serial",
    [
        (['180-0557-1-2b'], [2], ['121:452']),
        (None, None, None)
    ],
)
def test_get_srnum_input_validation(ar_serialnum, ar_installsize, ar_key_serial):
    '''
    Validates serial number input parameters

    Parameters
    ----------
    ar_serialnum : List of values
        DESCRIPTION.It specifies the serial number value to be processed.
    ar_installsize : Integer
        DESCRIPTION. Size of data

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.get_serialnumber(ar_serialnum, ar_installsize, ar_key_serial)
        assert info.type != ValueError


# Validate the output obtained from the test and actual results for df_couldnot


def test_get_srnum_output():
    '''
    Validates output of the inserted data against the expected output.

    Returns
    -------
    None.

    '''
    ar_serialnum = ['180-0557-1-2b']
    ar_installsize = [2]
    ar_key_serial = ['2455:458']
    actual_output1, actual_output2 = sr_num_class.get_serialnumber(
        ar_serialnum, ar_installsize, ar_key_serial)
    exp_op = pd.DataFrame()
    assert all([a == b for a, b in zip(actual_output2, exp_op)])


def test_get_srnum_empty_data():
    '''
    Validates output for empty dataframes

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.get_serialnumber(pd.DataFrame(), 0)
    assert info.type != ValueError


# Validate the output obtained from the test and actual results for df_out


def test_get_srnum_validoutput():
    '''
    Validates output of the inserted data against expected output for serial
    number field.

    Returns
    -------
    None.

    '''
    ar_serialnum = ['180-0557-1-1b']
    ar_installsize = [2]
    ar_key_serial = ['121:456']
    actual_out_test, actual_out_test1 = sr_num_class.get_serialnumber(
        ar_serialnum, ar_installsize, ar_key_serial)
    actual_output1 = actual_out_test['SerialNumber'].iloc[0]
    exp_op = '180-0557-1b'
    assert all([a == b for a, b in zip(actual_output1, exp_op)])


# Done- Validate if ar_srnum and second field is present.
# Validate if ls_decoded has unique or repeating values
# df_org['known_sr_num'] == False
# Done - If df_out and df_couoldnot has values or not
# Done - If ideal data can be processed,
# Process a sample case for df_couldNot type
# Code is not reachable for the if False condition at line 195


# %% Py tests for validate_srnum function validation.


@pytest.mark.parametrize(
    "ar_serialnum",
    [
        (['180-0557-1-2b']),
        (None)
    ],
)
def test_validate_srnum_input(ar_serialnum):
    '''
    Validates serial number input parameter

    Parameters
    ----------
    ar_serialnum : List of values
        DESCRIPTION.It specifies the serial number value to be processed.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.validate_srnum(ar_serialnum)
        assert info.type == Exception


def test_validate_srnum_empty_data():
    '''
    Validates output for empty dataframes

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.validate_srnum(pd.DataFrame(), 0)
    assert info.type != ValueError


# Corner cases such as case without the inserted values.
# try the prefix and postfix values for the entered data.
# Need to fix function params.

# %% known range function

@pytest.mark.parametrize(
    "df_input",
    [
        (['140-0063B&E']),
        (None),
        (pd.DataFrame(data={'SerialNumOrg': ['col1']})),
        (['180-0557a,b'])
    ],
)
def test_known_range_input_validate(df_input):
    '''
    Validates serial number data for varied range of values.

    Parameters
    ----------
    df_input : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.known_range(df_input)


def test_known_range_empty_data():
    '''
    Validates output for empty dataframes

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.known_range(pd.DataFrame())
    assert info.type != ValueError


# Method is not implemented. This function enhances testcase coverage mostly.

# %% unknown range function


@pytest.mark.parametrize(
    "df_input",
    [
        (['140-0063B&E']),
        (None),
        (pd.DataFrame(data={'SerialNumOrg': ['col1']})),
        (['180-0557a,b'])
    ],
)
def test_unknown_range_input_validate(df_input):
    '''
    Validates serial number data for range of values.

    Parameters
    ----------
    df_input : Pandas Dataframe
        DESCRIPTION.Serial number is passed to the function.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.unknown_range(df_input)


def test_unknown_range_empty_data():
    '''
    Validates output for empty dataframes

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.known_range(pd.DataFrame())
    assert info.type != ValueError


#  More corner cases to be included.
#  Method currently supports direct unit testcases.

# %% Validation for generate_seq_list function


def test_generate_seq_list_input():
    '''
    Validates input data for ideal case.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        ls_out_n = ['f_analyze', 'type', 'ix_beg',
                    'ix_end', 'pre_fix', 'post_fix']
        out = [True, 'list', '1144,110', '1144,110-1175', '110', '']
        dict_data = dict(zip(ls_out_n, out))

        sr_num_class.generate_seq_list(dict_data)
        assert info.type == Exception


def test_generate_seq_list_empty_data():
    '''
    Validates output for empty dataframes

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.generate_seq_list(pd.DataFrame())
    assert info.type != ValueError


# Covered basic testing approach.
# Include more cases depending upon the discussed cases

# %% Validation for generate_seq function
# self, out, sr_num, size


def test_generate_seq_input():
    '''
    Validates input data for ideal case scenario.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        out = [True, 'num', '1', '2', '180-0557-', 'b']
        sr_num = '180-0557-1-2b'
        size = 2
        sr_num_class.generate_seq(out, sr_num, size)
        assert info.type == Exception


def test_generate_seq_errorfunc():
    '''
    Validates input data for ideal case scenario.

    Returns
    -------
    None.

    '''
    # Failing case for '180-1059-1-24-fl'
    with pytest.raises(Exception) as info:
        out = [False, 'num', '', '', '', '']
        sr_num = '180-1059-1-24-fl'
        size = 24
        sr_num_class.generate_seq(out, sr_num, size)
        assert info.type == Exception


def test_generate_seq_alpha_char():
    '''
    Validate generate sequence function for alphabet range.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        out = [True, 'alpha', 'a', 'q', '180-05578', '']
        sr_num = '180-05578a-q'
        size = 17
        sr_num_class.generate_seq(out, sr_num, size)
        assert info.type == Exception


# %% Validation for letter function


@pytest.mark.parametrize(
    "seq_,size",
    [
        ("a", 2),
        (None, None),
        (0, 0)
    ])
def test_letter_range_input(seq_, size):
    '''
    Function validates if the datatype of input characters are passed in
    required format.

    Parameters
    ----------
    seq_ : Char
        DESCRIPTION. Character datatypes are passed.
    size : Int
        DESCRIPTION. Count to increment.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.letter_range(seq_, size)
        assert info.type != Exception


# Identify index function testing
@pytest.mark.parametrize(
    "seq_",
    [
        ("a"),
        (None),
        (0)
    ])
def test_identify_index_input(seq_):
    '''
    Function validates index based on input params

    Parameters
    ----------
    seq_ : Char
        DESCRIPTION. Character datatypes are passed.
    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.identify_index(seq_)
        assert info.type != Exception


# Covering the functionality for convert index
@pytest.mark.parametrize(
    "ix_n,pwr",
    [
        (None, None)
    ])
def test_convert_index_input(ix_n, pwr):
    '''
    Function validates if the datatype of input characters are passed in
    required format.

    Parameters
    ----------
     ix_n : Int
        DESCRIPTION. Integer datatypes are passed.
    size : Int
        DESCRIPTION. Count to increment.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.convert_index(ix_n, pwr)
        assert info.type != Exception


# %% Validation for identify sequence type function
vals = ['12017004-51-59,61', 10]


@pytest.mark.parametrize(
    "vals",
    [
        (['12017004-51-59,61', 10, '121:10']),
        (['180-0557-1-2b', 2, '121:10']),
        (['180-0557-b-c'], 2, '121:10'),
        (None, None, None),
        (['110-115'], 10, '121:10'),
        (['110-0631,1-2'], 2, '1254:78')
    ])
def test_identify_seq_input(vals):
    '''
    Validates the type of sequence based on input params.

    Parameters
    ----------
    vals : List
        DESCRIPTION. Contains list of values to be identified with a seq.

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.identify_seq_type(vals)
        assert info.type == Exception


def test_identify_seq_empty_data():
    '''
    Validates output for empty dataframes

    Returns
    -------
    None.

    '''
    with pytest.raises(Exception) as info:
        sr_num_class.identify_seq_type(pd.DataFrame())
    assert info.type == Exception

# %%
# test cases: 1. Data type
# 2. iF pd dataframe col name doesnt exists
# 3. data is empty 3.  ideal data 4. corner cases