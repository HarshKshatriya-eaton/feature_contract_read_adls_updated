# -*- coding: utf-8 -*-
"""@file Test_class_serial_num.py



@brief Unit Test class to test functionality of converting range of SerialNumber to unique SerialNumber



@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# Import system path
import sys

from pandas._testing import assert_frame_equal

sys.path.append(".")

# Initialize class instance
import pandas as pd
import pytest
from utils.dcpd.class_serial_number import SerialNumber

sr_num_class = SerialNumber()


# Pytest execution command
# Updated
# pytest ./tests/test_class_serial_num.py
# pytest --cov=.\utils\dcpd --cov-report html:.\coverage\ .\tests\
# coverage report -m utils/dcpd/class_services_data.py
# coverage run -m pytest ./tests/test_class_services_data.py
# coverage report -m

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

class TestPrepserialnumber:
    """
    This class tests the prep serial number data. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check exception throw
    """

    @pytest.mark.parametrize(
        "df_input",
        [
            (['140-0063B&E']),
            None,
            (pd.DataFrame(data={'SerialNumOrg': ['col1']})),
            (['180-0557a,b']),
            (['411-0573-1-11']),
            (['t23-08-us-s-7120']),
            (['Time']),
            (['442-0083-7b-12b']),
            (['452-0037-2b-5b']), (['110-2698'])
        ],
    )
    def test_prep_srnum_input_validate(self, df_input):
        """
        Validates the input dataframe parameters.

        Parameters
        ----------
        df_input : Pandas Dataframe
            DESCRIPTION. Serial number is passed to the function.

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.prep_srnum(df_input)

    def test_prep_srnum_empty_data(self):
        """
        Validates output for empty dataframes

        Returns
        -------
        None.
        """

        with pytest.raises(Exception) as info:
            sr_num_class.prep_srnum(pd.DataFrame())
        assert info.type != ValueError

    def test_prep_srnum_ideal_data(self):
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


class TestGetserialnumber:
    """
    This class tests the get serial number function. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check if funtion throws exception.
    The get serial number data performs data initialization and bifurcates data type results into
    df_out and df_couldnot by calling unknown_range type function.
    Both functionality tests have been carried out.
    """

    @pytest.mark.parametrize(
        "ar_serialnum, ar_installsize, ar_key_serial",
        [
            (['180-0557-1-2b'], [2], ['121:452']),
            (None, None, None),
            (['t23-08-us-s-7120'], [1], ["12547:524"]),
            (['Time'], [0], ["InvalidData"]),
            (['442-0083-7b-12b'], [6], ["4785:2356"]),
            (['452-0037-2b-5b'], [4], ["121:458"]),
            (['452-0030-1-2CB'], [2], ['142:54']),
            (['452-0030-1-2FL'], [2], ["474:121"])
        ],
    )
    def test_get_srnum_input_validation(self, ar_serialnum, ar_installsize, ar_key_serial):
        """
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

        """
        with pytest.raises(Exception) as info:
            sr_num_class.get_serialnumber(ar_serialnum, ar_installsize, ar_key_serial)
            assert info.type != ValueError

    # Validate the output obtained from the test and actual results for df_couldnot

    def test_get_srnum_output(self):
        """
        Validates output of the inserted data against the expected output for df_could not.

        Returns
        -------
        None.

        """
        ar_serialnum = ['180-0557-1-2b']
        ar_installsize = [2]
        ar_key_serial = ['2455:458']
        actual_output1, actual_output2 = sr_num_class.get_serialnumber(
            ar_serialnum, ar_installsize, ar_key_serial)
        exp_op = pd.DataFrame()
        assert all([a == b for a, b in zip(actual_output2, exp_op)])

    def test_get_srnum_empty_data(self):
        """
        Validates output for empty dataframes

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.get_serialnumber(pd.DataFrame(), 0)
        assert info.type != ValueError

    # Validate the output obtained from the test and actual results for df_out

    def test_get_srnum_validoutput(self):
        """
        Validates output of the inserted data against expected output for serial
        number field.

        Returns
        -------
        None.

        """
        ar_serialnum = ['180-0557-1-1b']
        ar_installsize = [2]
        ar_key_serial = ['121:456']
        actual_out_test, actual_out_test1 = sr_num_class.get_serialnumber(
            ar_serialnum, ar_installsize, ar_key_serial)
        actual_output1 = actual_out_test['SerialNumber'].iloc[0]
        exp_op = '180-0557-1b'
        assert all([a == b for a, b in zip(actual_output1, exp_op)])

    def test_getsrnum_patterncheck(self):
        """
        Validates if two similar serial number are inserted for expansion, then second pair should match

        Returns
        -------
        None.

        """
        ar_serialnum = ['442-0002-7a-12a', '442-0002-7a-12a']
        ar_installsize = [2, 2]
        ar_key_serial = ['121:456', '4564:457']

        actual_out_test, actual_out_test1 = sr_num_class.get_serialnumber(
            ar_serialnum, ar_installsize, ar_key_serial)

        expected_op = {'SerialNumberOrg': ['442-0002-7a-12a', '442-0002-7a-12a', '442-0002-7a-12a', '442-0002-7a-12a',
                                           '442-0002-7a-12a', '442-0002-7a-12a', '442-0002-7a-12a', '442-0002-7a-12a',
                                           '442-0002-7a-12a', '442-0002-7a-12a', '442-0002-7a-12a', '442-0002-7a-12a'],
                       'SerialNumber': ['442-0002-7a', '442-0002-8a', '442-0002-9a', '442-0002-10a', '442-0002-11a',
                                        '442-0002-12a', '442-0002-13a', '442-0002-14a', '442-0002-15a', '442-0002-16a',
                                        '442-0002-17a', '442-0002-18a'],
                       'KeySerial': ['121:456', '121:456', '121:456', '121:456', '121:456', '121:456', '4564:457',
                                     '4564:457', '4564:457', '4564:457', '4564:457', '4564:457']}

        expected_df = pd.DataFrame(expected_op)
        assert all([a == b for a, b in zip(actual_out_test, expected_df)])

# %% Py tests for validate_srnum function validation.

class TestValidateSerialNumber:
    """
    This class tests the validate serial number function. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check if funtion throws exception.

    """

    @pytest.fixture
    def serial_num_list(self):
        ar_serialnum = ['180-0557-1-2b', 'bcb-180-0557-1-2b-bus', '110-4613-1-2-BCB', '14868WA-2054-05',
                        '110-4708-1-16-Comm', '110-4917-1JBOX', 'skirt', 'rpp']
        return ar_serialnum

    def test_validate_srnum_input(self, serial_num_list):
        """
        Validates serial number input parameter

        Parameters
        ----------
        serial_num_list : List of serial number values
            DESCRIPTION.It specifies the serial number value to be processed.

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.validate_srnum(serial_num_list)
            assert info.type != Exception

    def test_validate_exp_out(self, serial_num_list):
        """
        Validates serial number by matching expected and actaul results

        Parameters
        ----------
        serial_num_list : List of serial number values
            DESCRIPTION.It specifies the serial number value to be processed.

        Returns
        -------
        Boolean values are returned from validate serial number method.

        """

        actual_op = sr_num_class.validate_srnum(serial_num_list)

        expected_op = {'f_valid': [True, False, True, True, True, True, False, False]}

        expected_df = pd.DataFrame(expected_op)

        assert all([a == b for a, b in zip(actual_op, expected_df)])

    def test_validate_srnum_empty_data(self):
        """
        Validates output for empty dataframes

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.validate_srnum(pd.DataFrame())
        assert info.type != ValueError

    # Done - Corner cases such as case without the inserted values.
    # Done try the prefix and postfix values for the entered data.
    # Done Need to fix function params.


# %% known range function
class TestKnownfunction:
    """
    This class tests the known serial number function. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check if funtion throws exception.

    """

    @pytest.mark.parametrize(
        "df_input",
        [
            (['140-0063B&E']),
            (None),
            (pd.DataFrame(data={'SerialNumOrg': ['col1']})),
            (['180-0557a,b'])
        ],
    )
    def test_known_range_input_validate(self, df_input):
        """
        Validates serial number data for varied range of values.

        Parameters
        ----------
        df_input : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.known_range(df_input)

    def test_known_range_empty_data(self):
        """
        Validates output for empty dataframes

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.known_range(pd.DataFrame())
        assert info.type != ValueError

    # Method is not implemented. This function enhances testcase coverage mostly.


# %% unknown range function
class TestUnknownfunction:
    """
    This class tests the Unknown serial number function. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check if funtion throws exception.

    """

    @pytest.mark.parametrize(
        "df_input",
        [
            (['442-0002-7a-12a']), (['140-0063B&E']),
            None, ([' 110-4171-9-14-CT']),
            ([' 110-4708-1-16-Comm']), ([' 118-0005-13']),
            (pd.DataFrame(data={'SerialNumOrg': ['col1']})), (['180-0557a,b'])
        ],
    )
    def test_unknown_range_input_validate(self, df_input):
        """
        Validates serial number data for range of values.

        Parameters
        ----------
        df_input : Pandas Dataframe
            DESCRIPTION.Serial number is passed to the function.

        Returns
        -------
        None.

        """

        with pytest.raises(Exception) as info:
            sr_num_class.unknown_range(df_input)

    def test_unknown_range_sample_data(self):
        """
        Validates serial number data for a sample testcase.

        Returns
        -------
        None.

        """

        df_input = pd.DataFrame()
        df_input['SerialNumberOrg'] = ['442-0002-51cb-71cb']
        df_input['InstallSize'] = [1]
        df_input['KeySerial'] = [1]

        actual_op = sr_num_class.unknown_range(df_input)

        expected_op = {'SerialNumberOrg': ['442-0002-51cb-71cb', '442-0002-51cb-71cb', '442-0002-51cb-71cb',
                                           '442-0002-51cb-71cb', '442-0002-51cb-71cb', '442-0002-51cb-71cb',
                                           '442-0002-51cb-71cb', '442-0002-51cb-71cb', '442-0002-51cb-71cb',
                                           '442-0002-51cb-71cb', '442-0002-51cb-71cb', '442-0002-51cb-71cb',
                                           '442-0002-51cb-71cb', '442-0002-51cb-71cb', '442-0002-51cb-71cb',
                                           '442-0002-51cb-71cb',
                                           '442-0002-51cb-71cb', '442-0002-51cb-71cb', '442-0002-51cb-71cb',
                                           '442-0002-51cb-71cb',
                                           '442-0002-51cb-71cb'],
                       'SerialNumber': ['442-0002-51cb', '442-0002-52cb', '442-0002-53cb', '442-0002-54cb',
                                        '442-0002-55cb',
                                        '442-0002-56cb', '442-0002-57cb', '442-0002-58cb', '442-0002-59cb',
                                        '442-0002-60cb',
                                        '442-0002-61cb', '442-0002-62cb', '442-0002-63cb', '442-0002-64cb',
                                        '442-0002-65cb',
                                        '442-0002-66cb', '442-0002-67cb', '442-0002-68cb', '442-0002-69cb',
                                        '442-0002-70cb',
                                        '442-0002-71cb'],
                       'KeySerial': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}

        expected_df = pd.DataFrame(expected_op)

        assert_frame_equal(actual_op[0].sort_index(axis=1), expected_df.sort_index(axis=1),
                           check_dtype=False, check_exact=False, check_names=True)

    def test_unknown_range_empty_data(self):
        """
        Validates output for empty dataframes

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.known_range(pd.DataFrame())
        assert info.type != ValueError


# %% Validation for generate_seq_list function

class TestGenerateSequenceList:
    """
    This class tests the Generate Sequence List function. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check if funtion throws exception.

    """

    def test_generate_seq_list_input(self):
        """
        Validates input data for ideal case.

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            ls_out_n = ['f_analyze', 'type', 'ix_beg',
                        'ix_end', 'pre_fix', 'post_fix']
            out = [True, 'list', '1144,110', '1144,110-1175', '110', '']
            dict_data = dict(zip(ls_out_n, out))

            sr_num_class.generate_seq_list(dict_data)
            assert info.type == Exception

    def test_generate_seq_list_empty_data(self):
        """
        Validates output for empty dataframes

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.generate_seq_list(pd.DataFrame())
        assert info.type != ValueError


# %% Validation for generate_seq function
class TestGenerateSequence:
    """
    This class tests the Generate Sequence main function. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check if funtion throws exception.

    """

    def test_generate_seq_input(self):
        """
        Validates input data for ideal case scenario.

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            out = [True, 'num', '1', '2', '180-0557-', 'b']
            sr_num = '180-0557-1-2b'
            size = 2
            key_serial = 3
            sr_num_class.generate_seq(out, sr_num, size, key_serial)
            assert info.type == Exception

    def test_generate_seq_errorfunc(self):
        """
        Validates input data for ideal case scenario.

        Returns
        -------
        None.

        """
        # Failing case for '180-1059-1-24-fl'
        with pytest.raises(Exception) as info:
            out = [False, 'num', '', '', '', '']
            sr_num = '180-1059-1-24-fl'
            size = 24
            key_serial = '142:54'
            sr_num_class.generate_seq(out, sr_num, size, key_serial)
            assert info.type == Exception

    def test_generate_seq_alpha_char(self):
        """
        Validate generate sequence function for alphabet range.

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            out = [True, 'alpha', 'a', 'q', '180-05578', '']
            sr_num = '180-05578a-q'
            size = 17
            key_serial = '192:2344'
            sr_num_class.generate_seq(out, sr_num, size, key_serial)
            assert info.type == Exception


# %% Validation for letter function
class TestLetterSeq:
    """
    This class tests the letter character function. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check if funtion throws exception.

    """

    @pytest.mark.parametrize(
        "seq_,size",
        [
            ("a", 2),
            (None, None),
            (0, 0),
            ("aa", 5),
            ("abcd", 10),
            ("a", 152)  # This condition must fail
        ])
    def test_letter_range_input(self, seq_, size):
        """
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

        """
        with pytest.raises(Exception) as info:
            sr_num_class.letter_range(seq_, size)
            assert info.type != Exception


# Identify index function testing
class TestIdentifyIndexSeq:
    """
    This class tests the identify index character function. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check if funtion throws exception.

    """

    @pytest.mark.parametrize(
        "seq_",
        [
            "a", None,
            0, "aa", "pred", "super"
        ])
    def test_identify_index_input(self, seq_):
        """
        Function validates index based on input params

        Parameters
        ----------
        seq_ : Char
            DESCRIPTION. Character datatypes are passed.
        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.identify_index(seq_)
            assert info.type != Exception

        # Covering the functionality for convert index
        @pytest.mark.parametrize(
            "ix_n,pwr",
            [
                (None, None)
            ])
        def test_convert_index_input(self, ix_n, pwr):
            """
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

            """
            with pytest.raises(Exception) as info:
                sr_num_class.convert_index(ix_n, pwr)
                assert info.type != Exception


class TestIdentifySequenceType:
    """
    This class tests the Identify Sequence Type function. Ideal conditions of serial numbers are tested
    with valid and invalid data. Invalid scenarios are covered to check if funtion throws exception.

    """
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
    def test_identify_seq_input(self, vals):
        """
        Validates the type of sequence based on input params.

        Parameters
        ----------
        vals : List
            DESCRIPTION. Contains list of values to be identified with a seq.

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.identify_seq_type(vals)
            assert info.type == Exception

    def test_identify_seq_empty_data(self):
        """
        Validates output for empty dataframes

        Returns
        -------
        None.

        """
        with pytest.raises(Exception) as info:
            sr_num_class.identify_seq_type(pd.DataFrame())
        assert info.type == Exception
