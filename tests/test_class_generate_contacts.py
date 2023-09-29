"""@file test_class_generate_contact_data.py.

@brief This file used to test code for generate contacts from contracts Data




@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
import pandas as pd
import pytest
from pandas._testing import assert_frame_equal, assert_series_equal
import numpy as np

from utils.dcpd.class_generate_contacts import Contacts

obj = Contacts()


class TestPrepData():
    """
    This Test class deals with the test cases for prep_data() method
    """

    @pytest.mark.parametrize(
        "df_data, dict_contact, error_type",
        [
            (
                    "", {"Company": ["Company_a", "Company_b"]},
                    "<class 'Exception'>"
            ),
            (
                    [], {"Company": ["Company_a", "Company_b"]},
                    "<class 'Exception'>"
            ),
            (
                    pd.DataFrame({"Company_b": ["DEF", "GHI"]}), "",
                    "<class 'TypeError'>"
            ),
            (
                    pd.DataFrame({"Company_b": ["DEF", "GHI"]}), [],
                    "<class 'TypeError'>"
            ),
            (
                    pd.DataFrame({"Company_b": ["DEF", "GHI"]}), None,
                    "<class 'TypeError'>"

            ),
            (
                    pd.DataFrame(), {"Company": ["Company_a", "Company_b"]},
                    "<class 'Exception'>"
            ),
            (
                    None, {"Company": ["Company_a", "Company_b"]},
                    "<class 'Exception'>"
            ),
            (
                    pd.DataFrame({"Name": ["a", "b"]}),
                    {"Company": ["Company_a", "Company_b"]},
                    "<class 'Exception'>"
            )
        ]
    )
    def test_errors(self, df_data, dict_contact, error_type):
        """
        This test cases checks all the invalid inputs for the prep_data()
        method and raises exceptions
        @param df_data: Input dataframe
        @param dict_contact: dictionary mapping columns from database to
        contacts field expected in output
        @param error_type: error type raised
        @return: None
        """
        with pytest.raises(Exception) as err:
            df_out, dict_updated = obj.prep_data(df_data, dict_contact)
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "df_data, dict_contact",
        [
            (
                    pd.DataFrame({"Name": ["a", "b"]}), {}
            )
        ]
    )
    def test_same_op(self, df_data, dict_contact):
        """
        This test cases checks all inputs for which dataframe remains unchanged
        @param df_data: Input dataframe
        @param dict_contact: dictionary mapping columns from database to
         contacts field expected in output
        @return: None
        """
        """
        Input df_data is the same as output df_data
        """
        df_out, dict_updated = obj.prep_data(df_data, dict_contact)
        assert_frame_equal(df_out, df_data)

    def test_valid_entries(self):
        """
        This method checks the output for valid inputs for prep_data() method
        @return: None
        """
        df_data = pd.DataFrame({
            "Company_a": ["a", "a", np.nan, "a", np.nan],
            "Company_b": ["b", "a", "c", np.nan, np.nan],
            "exp_out": ["a; b", "a", "c", "a", ""]
        })

        dict_contact = {"Company": ["Company_a", "Company_b"]}
        df_data, dict_updated = obj.prep_data(df_data, dict_contact)

        # assert_series_equal(df_data["exp_out"].to, df_data["nc_Company"])
        assert (df_data["exp_out"] == df_data["nc_Company"]).all()


class TestSerialNumber():
    """
        This Test class deals with the test cases for serial_num() method
    """
    @pytest.mark.parametrize(
        "description, error_type",
        [
            ([], "<class 'TypeError'>"),
            (pd.DataFrame(), "<class 'TypeError'>"),
            (None, "<class 'TypeError'>")
        ]
    )
    def test_errors(self, description, error_type):
        """
        This test cases checks all the invalid inputs for the serial_num()
        method and raises exceptions
        @param description: Input String
        @param error_type: error type raised
        @return: None
        """
        with pytest.raises(Exception) as err:
            sr_num = obj.serial_num(description)
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "description",
        [
            ("")
        ]
    )
    def test_empty_op(self, description):
        """
        This method checks the inputs for which op is empty
        @param description: Input String
        @return: None
        """
        sr_num = obj.serial_num(description)
        assert sr_num == []

    @pytest.mark.parametrize(
        "description, op_pattern",
        [
            ("a-b", ["a-b"]),
            ("a-b-c", ["a-b-c"]),
            ("a-b-c-d-e", ["a-b-c-d-e"]),
            ("a-b-c d-e", ["a-b-c", "d-e"]),
            ("a-b-c, RANDOM TEXT d-e", ["a-b-c", "d-e"]),
            ("PRE a-bc", ["a-bc"]),
            ("a-bc POST", ["a-bc"])
        ]
    )
    def test_valid_entries(self, description, op_pattern):
        """
        This test case checks the output for valid inputs
        @param description: Input String
        @param op_pattern: Expected op
        @return:  None
        """
        sr_num = obj.serial_num(description)
        assert sr_num == op_pattern


class TestFilterLatest():
    """
    This Test class deals with the test cases for filter_latest() method
    """
    @pytest.mark.parametrize(
        "df, error_type",
        [
            ([], "<class 'Exception'>"),
            ("", "<class 'Exception'>"),
            (None, "<class 'Exception'>"),
            (
                pd.DataFrame(), "<class 'Exception'>"
            ),
            (
                pd.DataFrame({
                    "Name": ["a", "b"]
                }),
                "<class 'Exception'>"
            ),
            (
                pd.DataFrame({
                    "Name": ["a", "b"],
                    "Age": ["A", "B"],
                    "Address": ["C", "D"]
                }),
                "<class 'Exception'>"
            )
        ]
    )
    def test_errors(self, df, error_type):
        """
        This test cases checks all the invalid inputs for the filter_latest()
        @param df: input dataframe
        @param error_type: error_type raised
        @return: None
        """
        with pytest.raises(Exception) as err:
            df = obj.filter_latest(df)
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "df, df_out",
        [
            (
                    pd.DataFrame({
                        "SerialNumber": ["a"],
                        "Source": ["A"],
                        "Contact_Type": ["C"],
                        "Date": ["1/1/2023"]
                    }),
                    pd.DataFrame({
                        "SerialNumber": ["a"],
                        "Source": ["A"],
                        "Contact_Type": ["C"],
                        "Date": ["1/1/2023"]
                    })
            ),
            (
                    pd.DataFrame({
                        "SerialNumber": ["a", "b"],
                        "Source": ["A", "B"],
                        "Contact_Type": ["C", "D"],
                        "Date": ["1/1/2023", "1/1/2022"]
                    }),
                    pd.DataFrame({
                        "SerialNumber": ["b", "a"],
                        "Source": ["B", "A"],
                        "Contact_Type": ["D", "C"],
                        "Date": ["1/1/2022", "1/1/2023"]
                    })
            ),
            (
                    pd.DataFrame({
                        "SerialNumber": ["b", "a"],
                        "Source": ["A", "B"],
                        "Contact_Type": ["C", "D"],
                        "Date": ["1/1/2023", "1/1/2022"]
                    }),
                    pd.DataFrame({
                        "SerialNumber": ["b", "a"],
                        "Source": ["A", "B"],
                        "Contact_Type": ["C", "D"],
                        "Date": ["1/1/2023", "1/1/2022"]
                    }),
            ),
            (
                    pd.DataFrame({
                        "SerialNumber": ["b", "a", "a", "a", "a"],
                        "Source": ["A", "B", "C", "C", "C"],
                        "Contact_Type": ["D", "D", "D", "E", "E"],
                        "Date": [
                            "1/1/2022", "1/1/2022", "1/1/2022", "1/1/2023",
                            "1/1/2022"
                        ]
                    }),
                    pd.DataFrame({
                        "SerialNumber": ["b", "a", "a", "a"],
                        "Source": ["A", "C", "C", "B"],
                        "Contact_Type": ["D", "E", "D", "D"],
                        "Date": [
                            "1/1/2022", "1/1/2023", "1/1/2022", "1/1/2022"
                        ]
                    })
            ),
        ]
    )
    def test_valid_entries(self, df, df_out):
        """
        Method to test the output of filter_latest() method for valid entries
        @param df: input dataframe
        @param df_out: expected dataframe
        @return: None
        """
        df = obj.filter_latest(df)
        df.reset_index(inplace=True)
        df = df.drop("index", axis=1)
        assert_frame_equal(df, df_out)


class TestPostProcess():
    """
        This Test class deals with the test cases for post_process() method
    """
    @pytest.mark.parametrize(
        "df, error_type",
        [
            ([], "<class 'Exception'>"),
            (pd.DataFrame(), "<class 'Exception'>"),
            (None, "<class 'Exception'>"),
            (
                    pd.DataFrame({
                        "Name": ["a", "b"]
                    }),
                    "<class 'Exception'>"
            ),
            (
                    pd.DataFrame({
                        "Name": ["a", "b"],
                        "Email": ["a", "b"],
                        "Company_Phone": ["a", "b"],
                        "Serial Number": ["a", "b"],
                        "Source": ["a", "b"],
                        "Date": ["a", "b"],
                    }),
                    "<class 'Exception'>"
            ),
            (
                    pd.DataFrame({
                        "Name": ["a", "b"],
                        "Email": ["a", "b"],
                        "Company_Phone": ["a", "b"],
                        "Serial Number": ["a", "b"],
                        "Source": ["a", "b"],
                        "Contact_Type": ["a", "b"]
                    }),
                    "<class 'Exception'>"
            ),
        ]
    )
    def test_errors(self, df, error_type):
        """
        This test cases checks all the invalid inputs for the post_process()
        @param df: input dataframe
        @param error_type: expected error_type
        @return: None
        """
        with pytest.raises(Exception) as err:
            df = obj.post_process(df)
        assert error_type == str(err.type)


class TestValidateOp():
    """
    This Test class deals with the test cases for validate_op() method
    """
    @pytest.mark.parametrize(
        "df, error_type",
        [
            ([], "<class 'AttributeError'>"),
            ("pd.DataFrame()", "<class 'AttributeError'>"),
            (pd.DataFrame(), "<class 'ValueError'>"),
            (None, "<class 'AttributeError'>"),
            (
                    pd.DataFrame({
                        "Name": ["a", "b"]
                    }),
                    "<class 'KeyError'>"
            )
        ]
    )
    def test_errors(self, df, error_type):
        """
        This test cases checks all the invalid inputs for the validate_op()
        @param df: input dataframe
        @param error_type: expected error_type
        @return: None
        """
        with pytest.raises(Exception) as err:
            df = obj.validate_op(df)
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "df, df_out",
        [
            (
                    pd.DataFrame({
                        "Name": ["trial 1"],
                        "Email": ["abc@domain.com"],
                        "Company_Phone": ["123-456-7890"],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame({
                        "Name": ["trial 1"],
                        "Email": ["abc@domain.com"],
                        "Company_Phone": ["123-456-7890"],
                        "SerialNumber": ["452-0071-2"]
                    })
            ),
            (
                    pd.DataFrame({
                        "Name": ["trial 1"],
                        "Email": [""],
                        "Company_Phone": [""],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame({
                        "Name": ["trial 1"],
                        "Email": [""],
                        "Company_Phone": [""],
                        "SerialNumber": ["452-0071-2"]
                    }),
            ),
            (
                    pd.DataFrame({
                        "Name": [""],
                        "Email": ["abc@domain.com"],
                        "Company_Phone": [""],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame({
                        "Name": [""],
                        "Email": ["abc@domain.com"],
                        "Company_Phone": [""],
                        "SerialNumber": ["452-0071-2"]
                    })
            ),
            (
                    pd.DataFrame({
                        "Name": [""],
                        "Email": [""],
                        "Company_Phone": ["123-456-7890"],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame({
                        "Name": [""],
                        "Email": [""],
                        "Company_Phone": ["123-456-7890"],
                        "SerialNumber": ["452-0071-2"]
                    })
            ),
            (
                    pd.DataFrame({
                        "Name": [None],
                        "Email": [None],
                        "Company_Phone": [None],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame(columns=[
                        "Name", "Email", "Company_Phone", "SerialNumber"
                    ])
            ),
            (
                    pd.DataFrame({
                        "Name": [""],
                        "Email": [""],
                        "Company_Phone": [""],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame(columns=[
                        "Name", "Email", "Company_Phone", "SerialNumber"
                    ])
            ),
            (
                    pd.DataFrame({
                        "Name": [""],
                        "Email": [""],
                        "Company_Phone": ["(+1)"],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame(columns=[
                        "Name", "Email", "Company_Phone", "SerialNumber"
                    ])
            ),
            (
                    pd.DataFrame({
                        "Name": ["trial1-"],
                        "Email": [""],
                        "Company_Phone": [""],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame({
                        "Name": ["trial1"],
                        "Email": [""],
                        "Company_Phone": [""],
                        "SerialNumber": ["452-0071-2"]
                    }),
            ),
            (
                    pd.DataFrame({
                        "Name": [None],
                        "Email": ["abc@domain.com"],
                        "Company_Phone": [""],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame({
                        "Name": [None],
                        "Email": ["abc@domain.com"],
                        "Company_Phone": [""],
                        "SerialNumber": ["452-0071-2"]
                    })
            ),
            (
                    pd.DataFrame({
                        "Name": ["pqr"],
                        "Email": [None],
                        "Company_Phone": [""],
                        "Serial Number": ["452-0071-2"]
                    }),
                    pd.DataFrame({
                        "Name": ["pqr"],
                        "Email": [None],
                        "Company_Phone": [""],
                        "SerialNumber": ["452-0071-2"]
                    })
            ),
        ]
    )
    def test_valid_entries(self, df, df_out):
        """
        This test case verifies the output for diffrent valid entries to method
        validate_op()
        @param df: input dataframe
        @param df_out: Expected output dataframe
        @return: None
        """
        df = obj.validate_op(df)
        df_out = df_out.fillna("")
        assert_frame_equal(df, df_out)


class TestExtractData():
    """
    This Test class deals with the test cases for extract_data() method
    """
    @pytest.mark.parametrize(
        "dict_src, df_data, error_type",
        [
            ([], pd.DataFrame(), "<class 'Exception'>"),
            (pd.DataFrame(), pd.DataFrame(), "<class 'Exception'>"),
            ("events", [], "<class 'Exception'>"),
            ("events", "dataframe", "<class 'Exception'>"),
            (None, pd.DataFrame(), "<class 'Exception'>"),
            ([], None, "<class 'Exception'>"),
            (pd.DataFrame(), None, "<class 'Exception'>"),
            (None, None, "<class 'Exception'>"),
            (
                "events",
                pd.DataFrame({"Company_b": ["DEF", "GHI"]}),
                "<class 'Exception'>"
            ),
        ]
    )
    def test_errors(self, dict_src, df_data, error_type):
        """
        This test cases checks all the invalid inputs for the extract_data()
        @param dict_src: input source type
        @param df_data: input dataframe
        @param error_type: expected error_type
        @return: None
        """
        with pytest.raises(Exception) as err:
            df_data = obj.extract_data(dict_src, df_data)
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "dict_src, df_data",
        [
            ("contracts", pd.DataFrame()),
            ("PM", pd.DataFrame()),
            ("Renewel", pd.DataFrame()),
            ("Services", pd.DataFrame()),
        ]
    )
    def test_same_op(self, dict_src, df_data):
        """
        This test cases checks all the inputs for which dataframe remains
        unchanged.
        @param dict_src: input source type
        @param df_data: input dataframe
        @return: None
        """
        df_out = obj.extract_data(dict_src, df_data)
        assert_frame_equal(df_out, df_data)

    def test_config_not_load(self):
        """
        This test cases checks all the inputs for which the method can't find
        dependent files.
        @param dict_src: input source type
        @param df_data: input dataframe
        @return: None
        """
        usa_states = (
            obj.config['output_contacts_lead']["usa_states"]
        )
        del obj.config['output_contacts_lead']["usa_states"]
        with pytest.raises(Exception) as _:
            df_out = obj.extract_data(
                "events", pd.DataFrame({"Description": "abcde"})
            )
        obj.config['output_contacts_lead']["usa_states"] = usa_states

    @pytest.mark.parametrize(
        "dict_src, df_data, df_out",
        [
            (
                    "events",
                    pd.DataFrame({
                        "Description": [" poc Jhon Doe 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": [" poc Jhon Doe 118-0023-206"],
                        "contact_name": ["Jhon Doe "],
                        "contact": [None],
                        "email": [None],
                        "address": [None],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": [" contact Jhon Doe 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": [" contact Jhon Doe 118-0023-206"],
                        "contact_name": ["Jhon Doe "],
                        "contact": [None],
                        "email": [None],
                        "address": [None],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": [" contact there Jhon Doe 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": [
                            " contact there Jhon Doe 118-0023-206"],
                        "contact_name": ["Jhon Doe "],
                        "contact": [None],
                        "email": [None],
                        "address": [None],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": [
                            " +91-123-456-7891 Jhon Doe 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": [
                            " +91-123-456-7891 Jhon Doe 118-0023-206"],
                        "contact_name": ["Jhon Doe"],
                        "contact": ["91-123-456-7891"],
                        "email": [None],
                        "address": [" +91-123-456-7891 Jhon Doe 118-0023-206"],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": [
                            "123-456-7891 Jhon Doe 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": [
                            "123-456-7891 Jhon Doe 118-0023-206"],
                        "contact_name": ["Jhon Doe"],
                        "contact": ["123-456-7891"],
                        "email": [None],
                        "address": ["123-456-7891 Jhon Doe 118-0023-206"],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": [
                            "1234567891 Jhon Doe 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": [
                            "1234567891 Jhon Doe 118-0023-206"],
                        "contact_name": [None],
                        "contact": ["1234567891"],
                        "email": [None],
                        "address": ["1234567891 Jhon Doe 118-0023-206"],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": [
                            "-1234567891 Jhon Doe 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": [
                            "-1234567891 Jhon Doe 118-0023-206"],
                        "contact_name": [None],
                        "contact": ["1234567891"],
                        "email": [None],
                        "address": ["-1234567891 Jhon Doe 118-0023-206"],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": [
                            "1234567891 test_mail@domain.com Jhon Doe"
                            " 118-0023-206"
                        ]
                    }),
                    pd.DataFrame({
                        "Description": [
                            "1234567891 test_mail@domain.com Jhon Doe"
                            " 118-0023-206"
                        ],
                        "contact_name": [None],
                        "contact": ["1234567891"],
                        "email": ["test_mail@domain.com"],
                        "address": [
                            "1234567891 test_mail@domain.com Jhon Doe"
                            " 118-0023-206"
                        ],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": [
                            "1234567891 test_mail@check.uni.au Jhon Doe"
                            " 118-0023-206"
                        ]
                    }),
                    pd.DataFrame({
                        "Description": [
                            "1234567891 test_mail@check.uni.au Jhon Doe"
                            " 118-0023-206"
                        ],
                        "contact_name": [None],
                        "contact": ["1234567891"],
                        "email": ["test_mail@check.uni.au"],
                        "address": [
                            "1234567891 test_mail@check.uni.au Jhon Doe"
                            " 118-0023-206"
                        ],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": ["1234567891 a@b.c 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": ["1234567891 a@b.c 118-0023-206"],
                        "contact_name": [None],
                        "contact": ["1234567891"],
                        "email": ["a@b.c"],
                        "address": ["1234567891 a@b.c 118-0023-206"],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
            (
                    "events",
                    pd.DataFrame({
                        "Description": ["1234567891 a1@b#.c* 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": ["1234567891 a1@b#.c* 118-0023-206"],
                        "contact_name": [None],
                        "contact": ["1234567891"],
                        "email": ["a1@b#.c*"],
                        "address": ["1234567891 a1@b#.c* 118-0023-206"],
                        "SerialNumber": ["118-0023-206"]
                    })
            ),
        ]
    )
    def test_valid_entries(self, dict_src, df_data, df_out):
        """
        This test cases verifies the op for all the valid inputs
        @param dict_src: input source type
        @param df_data: input dataframe
        @param df_out: Expected dataframe
        @return: None
        """
        df_data = obj.extract_data(
            "events", df_data
        )
        df_out = df_out.fillna("")
        assert_frame_equal(df_out, df_data)


class TestExceptionSrc():
    """
    This Test class deals with the test cases for exception_src() method
    """
    @pytest.mark.parametrize(
        "dict_src, df_data, dict_contact, error_type",
        [
            ([], pd.DataFrame(), {}, "<class 'TypeError'>"),
            (pd.DataFrame(), pd.DataFrame(), {}, "<class 'TypeError'>"),
            ("events", [], {}, "<class 'TypeError'>"),
            ("events", "pd.DataFrame()", {}, "<class 'TypeError'>"),
            ("events", pd.DataFrame(), "str", "<class 'TypeError'>"),
            ("events", pd.DataFrame(), pd.DataFrame(), "<class 'TypeError'>"),
            (None, pd.DataFrame(), {}, "<class 'TypeError'>"),
            ("events", None, {}, "<class 'TypeError'>"),
            ("events", pd.DataFrame(), None, "<class 'TypeError'>"),
        ]
    )
    def test_error(
            self, dict_src, df_data, dict_contact, error_type
    ):
        """
        This test cases checks all the invalid inputs for the exception_src()
        @param dict_src: input source type
        @param df_data: input dataframe
        @param dict_contact: Mapping columns to output field names.
        @param error_type: expected error_type
        @return: None
        """
        with pytest.raises(Exception) as err:
            df_out, dict_updated = obj.exception_src(
                dict_src, df_data, dict_contact
            )
        assert error_type == str(err.type)

    @pytest.mark.parametrize(
        "dict_src, df_data, dict_contact",
        [
            ("Renewal", pd.DataFrame(), {}),
            ("PM", pd.DataFrame(), {}),
            ("random", pd.DataFrame(), {})
        ]
    )
    def test_same_op(self, dict_src, df_data, dict_contact):
        """
        This test cases checks all the values for which the op remains
        unchanged from innut
        @param dict_src: input source type
        @param df_data: input dataframe
        @param dict_contact: Mapping columns to output field names.
        @return: None
        """
        df_out, dict_updated = obj.exception_src(
            dict_src, df_data, dict_contact
        )
        assert_frame_equal(df_out, df_data)

    @pytest.mark.parametrize(
        "dict_src, df_data, dict_contact",
        [
            ("services", pd.DataFrame(), {}),
            ("contracts", pd.DataFrame(), {})
        ]
    )
    def test_config_not_load(
            self, dict_src, df_data, dict_contact
    ):
        """
        This test cases checks all the values for which the method can't find
        relevant config files
        @param dict_src: input source type
        @param df_data: input dataframe
        @param dict_contact: Mapping columns to output field names.
        @return: None
        """
        file_name = obj.config['file']['Processed'][dict_src]['file_name']
        del obj.config['file']['Processed'][dict_src]['file_name']

        with pytest.raises(Exception) as _:
            df_out, dict_updated = obj.exception_src(
                dict_src, df_data, dict_contact
            )

        obj.config['file']['Processed'][dict_src]['file_name'] = file_name


if __name__ == "__main__":
    prep_data_obj = TestPrepData()
    serial_num_obj = TestSerialNumber()
    filter_latest_obj = TestFilterLatest()
    post_process_obj = TestPostProcess()
    validate_op_obj = TestValidateOp()
    extract_data_obj = TestExtractData()
    exception_src_obj = TestExceptionSrc()
