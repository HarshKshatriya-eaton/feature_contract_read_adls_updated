"""@file test_events_description_extraction.py.

@brief This file used to test code for Extraction of information from events
data

@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
import pandas as pd
import pytest

from utils.contacts_fr_events_data import DataExtraction
from utils import IO

data_extractor = DataExtraction()
config = IO.read_json(mode="local", config={
            "file_dir": "./references/", "file_name": "config_dcpd.json"})


class TestExtract:
    """
    Checks for the contacts extracted from the events data
    """

    @pytest.mark.parametrize(
        "description, error_type",
        [
            (123, "<class 'TypeError'>"),
            ([], "<class 'TypeError'>"),
            (pd.DataFrame(), "<class 'TypeError'>")
        ]
    )
    def test_errors_name(self, description, error_type):
        """
        This test cases checks errors raised all the invalid inputs for the
        extract_contact_name()
        @param dict_src: input source type
        @param df_data: input dataframe
        @param error_type: expected error_type
        @return: None
        """
        with pytest.raises(Exception) as err:
            df_data = data_extractor.extract_contact_name(description)
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "description, expected_name",
        [
            (" contact there Jhon Doe 118-0023-206", "Jhon Doe"),
            (" +91-123-456-7891 Jhon Doe 118-0023-206", "Jhon Doe")
        ]
    )
    def test_extract_name(self, description, expected_name):
        """
        This test cases checks the extraction of contact names from the given
        string
        """
        ac_name = data_extractor.extract_contact_name(description)
        assert expected_name == ac_name

    @pytest.mark.parametrize(
        "description, error_type",
        [
            (123, "<class 'TypeError'>"),
            ([], "<class 'TypeError'>"),
            (pd.DataFrame(), "<class 'TypeError'>")
        ]
    )
    def test_errors_contact(self, description, error_type):
        """
        This test cases checks errors raised all the invalid inputs for the
        extract_contact_name()
        @param dict_src: input source type
        @param df_data: input dataframe
        @param error_type: expected error_type
        @return: None
        """
        with pytest.raises(Exception) as err:
            df_data = data_extractor.extract_contact_no(description)
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "description, expected_name",
        [
            (" +91-123-456-7891 Jhon Doe 118-0023-206", "91-123-456-7891"),
            ("123-456-7891 Jhon Doe 118-0023-206", "123-456-7891")
        ]
    )
    def test_extract_contact_no(self, description, expected_name):
        """
        This test cases checks the extraction of contact names from the given
        string
        """
        ac_name = data_extractor.extract_contact_no(description)
        assert expected_name == ac_name

    @pytest.mark.parametrize(
        "description",
        [
            (" contact there Jhon Doe 118-0023-206"),
            ("")
        ]
    )
    def test_null_contact_no(self, description):
        """
        This test cases checks whether Null value is extracted for the given
        inputs
        """
        ac_name = data_extractor.extract_contact_no(description)
        assert not ac_name

    @pytest.mark.parametrize(
        "description, error_type",
        [
            (123, "<class 'TypeError'>"),
            ([], "<class 'TypeError'>"),
            (pd.DataFrame(), "<class 'TypeError'>")
        ]
    )
    def test_errors_email(self, description, error_type):
        """
        This test cases checks errors raised all the invalid inputs for the
        extract_email()
        @param dict_src: input source type
        @param df_data: input dataframe
        @param error_type: expected error_type
        @return: None
        """
        with pytest.raises(Exception) as err:
            df_data = data_extractor.extract_email(description)
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "description, expected_email",
        [
            ("1234567891 a1@b#.c* 118-0023-206", "a1@b#.c*"),
            ("1234567891 a@b.c 118-0023-206", "a@b.c")
        ]
    )
    def test_extract_email(self, description, expected_email):
        """
        This test cases checks the extraction of contact names from the given
        string
        """
        ac_email = data_extractor.extract_email(description)
        assert expected_email == ac_email

    @pytest.mark.parametrize(
        "description, error_type",
        [
            (123, "<class 'TypeError'>"),
            ([], "<class 'TypeError'>"),
            (pd.DataFrame(), "<class 'TypeError'>")
        ]
    )
    def test_errors_address(self, description, error_type):
        """
        This test cases checks errors raised all the invalid inputs for the
        extract_address()
        @param dict_src: input source type
        @param df_data: input dataframe
        @param error_type: expected error_type
        @return: None
        """
        usa_states = config['output_contacts_lead']["usa_states"]
        pat_state_short = ' ' + ' | '.join(list(usa_states.keys())) + ' '
        pat_state_long = ' ' + ' | '.join(list(usa_states.values())) + ' '
        pat_address = str.lower(
            '(' + pat_state_short + '|' + pat_state_long + ')')
        with pytest.raises(Exception) as err:
            df_data = data_extractor.extract_address(description, pat_address)
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "description, expected_address",
        [
            (
                "John Doe, 12, random square, any city",
                "John Doe, 12, random square, any city"
            ),
        ]
    )
    def test_errors_address(self, description, expected_address):
        """
        This test cases checks the extraction of contact address from the given
        string
        """
        usa_states = config['output_contacts_lead']["usa_states"]
        pat_state_short = ' ' + ' | '.join(list(usa_states.keys())) + ' '
        pat_state_long = ' ' + ' | '.join(list(usa_states.values())) + ' '
        pat_address = str.lower(
            '(' + pat_state_short + '|' + pat_state_long + ')')
        ac_address = data_extractor.extract_address(description, pat_address)
        assert ac_address == expected_address


if __name__ == "__main__":
    obj_contact = TestExtract()
