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

    @pytest.mark.parametrize(
        "df_data, dict_contact",
        [
            (
                "", {"Company": ["Company_a", "Company_b"]}
            ),
            (
                [], {"Company": ["Company_a", "Company_b"]}
            ),
            (
                pd.DataFrame({"Company_b": ["DEF", "GHI"]}), ""
            ),
            (
                pd.DataFrame({"Company_b": ["DEF", "GHI"]}), []
            ),
            (
                pd.DataFrame({"Company_b": ["DEF", "GHI"]}), None
            ),
            (
                pd.DataFrame(), {"Company": ["Company_a", "Company_b"]}
            ),
            (
                None, {"Company": ["Company_a", "Company_b"]}
            ),
            (
                pd.DataFrame({"Name": ["a", "b"]}),
                {"Company": ["Company_a", "Company_b"]}
            )
        ]
    )
    def test_prep_data_errors(self, df_data, dict_contact):
        """
            The provided inputs raise an error while executing
        """
        with pytest.raises(Exception) as _:
            df_out, dict_updated = obj.prep_data(df_data, dict_contact)

    @pytest.mark.parametrize(
        "df_data, dict_contact",
        [
            (
                pd.DataFrame({"Name": ["a", "b"]}), {}
            )
        ]
    )
    def test_prep_data_same_op(self, df_data, dict_contact):
        """
        Input df_data is the same as output df_data
        """
        df_out, dict_updated = obj.prep_data(df_data, dict_contact)
        assert_frame_equal(df_out, df_data)

    def test_prep_data_valid_entries(self):
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

    @pytest.mark.parametrize(
        "description",
        [
            ([]),
            (pd.DataFrame()),
            (None)
        ]
    )
    def test_sr_num_errors(self, description):
        """
            The provided inputs raise an error while executing
        """
        with pytest.raises(Exception) as _:
            sr_num = obj.serial_num(description)

    @pytest.mark.parametrize(
        "description",
        [
            ("")
        ]
    )
    def test_sr_num_empty_op(self, description):
        """
        Input df_data is the same as output df_data
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
    def test_sr_num_valid_entries(self, description, op_pattern):
        """
        Input df_data is the same as output df_data
        """
        sr_num = obj.serial_num(description)
        assert sr_num == op_pattern

class TestFilterLatest():

    @pytest.mark.parametrize(
        "df",
        [
            ([]),
            (""),
            (),
            (None),
            (
                pd.DataFrame()
            ),
            (
                pd.DataFrame({
                    "Name": ["a", "b"]
                })
            ),
            (
                pd.DataFrame({
                    "Name": ["a", "b"],
                    "Age": ["A", "B"],
                    "Address": ["C", "D"]
                })
            )
        ]
    )
    def test_filter_latest_errors(self, df):
        """
            The provided inputs raise an error while executing
        """
        with pytest.raises(Exception) as _:
            df = obj.filter_latest(df)

    @pytest.mark.parametrize(
        "df, df_out",
        [
            (
                pd.DataFrame({
                    "Serial Number": ["a"],
                    "Source": ["A"],
                    "Contact_Type": ["C"],
                    "Date": ["1/1/2023"]
                }),
                pd.DataFrame({
                    "Serial Number": ["a"],
                    "Source": ["A"],
                    "Contact_Type": ["C"],
                    "Date": ["1/1/2023"]
                })
            ),
            (
                pd.DataFrame({
                    "Serial Number": ["a", "b"],
                    "Source": ["A", "B"],
                    "Contact_Type": ["C", "D"],
                    "Date": ["1/1/2023", "1/1/2022"]
                }),
                pd.DataFrame({
                    "Serial Number": ["b", "a"],
                    "Source": ["B", "A"],
                    "Contact_Type": ["D", "C"],
                    "Date": ["1/1/2022", "1/1/2023"]
                })
            ),
            (
                pd.DataFrame({
                    "Serial Number": ["b", "a"],
                    "Source": ["A", "B"],
                    "Contact_Type": ["C", "D"],
                    "Date": ["1/1/2023", "1/1/2022"]
                }),
                pd.DataFrame({
                    "Serial Number": ["b", "a"],
                    "Source": ["A", "B"],
                    "Contact_Type": ["C", "D"],
                    "Date": ["1/1/2023", "1/1/2022"]
                }),
            ),
            (
                pd.DataFrame({
                    "Serial Number": ["b", "a", "a", "a", "a"],
                    "Source": ["A", "B", "C", "C", "C"],
                    "Contact_Type": ["D", "D", "D", "E", "E"],
                    "Date": [
                        "1/1/2022", "1/1/2022", "1/1/2022", "1/1/2023",
                        "1/1/2022"
                    ]
                }),
                pd.DataFrame({
                    "Serial Number": ["b", "a", "a", "a"],
                    "Source": ["A", "C", "C", "B"],
                    "Contact_Type": ["D", "E", "D", "D"],
                    "Date": [
                        "1/1/2022", "1/1/2023", "1/1/2022", "1/1/2022"
                    ]
                })
            ),
        ]
    )
    def test_filter_latest_valid_entries(self, df, df_out):
        """
            The provided inputs raise an error while executing
        """
        df = obj.filter_latest(df)
        df.reset_index(inplace=True)
        df = df.drop("index", axis=1)
        assert_frame_equal(df, df_out)

class TestPostProcess():

    @pytest.mark.parametrize(
        "df",
        [
            ([]),
            ("pd.DataFrame()"),
            (pd.DataFrame()),
            (None),
            (
                pd.DataFrame({
                    "Name": ["a", "b"]
                })
            ),
            (
                pd.DataFrame({
                    "Name": ["a", "b"],
                    "Email": ["a", "b"],
                    "Company_Phone": ["a", "b"],
                    "Serial Number": ["a", "b"],
                    "Source": ["a", "b"],
                    "Date": ["a", "b"],
                })
            ),
            (
                pd.DataFrame({
                    "Name": ["a", "b"],
                    "Email": ["a", "b"],
                    "Company_Phone": ["a", "b"],
                    "Serial Number": ["a", "b"],
                    "Source": ["a", "b"],
                    "Contact_Type": ["a", "b"]
                })
            ),
        ]
    )
    def test_post_process_errors(self, df):
        """
            The provided inputs raise an error while executing
        """
        with pytest.raises(Exception) as _:
            df = obj.post_process(df)

    @pytest.mark.parametrize(
        "df, df_out",
        [
            (
                pd.DataFrame({
                    "Name": ["a", "b"],
                    "Email": ["a", "None"],
                    "Company_Phone": ["None", "b"],
                    "Serial Number": ["a", "a"],
                    "Source": ["a", "a"],
                    "Contact_Type": ["a", "a"],
                    "Date": ["a", "a"],
                }),
                pd.DataFrame({
                    "Name": ["a", "b"],
                    "Email": ["a", "None"],
                    "Company_Phone": ["None", "b"],
                    "Serial Number": ["a", "a"],
                    "Source": ["a", "a"],
                    "Contact_Type": ["a", "a"],
                    "Date": ["a", "a"],
                })
            ),
        ]
    )
    def test_post_process_valid_entries(self, df, df_out):
        """
        Input df_data is the same as output df_data
        """
        # df = obj.post_process(df)
        # print(df)
        # assert_frame_equal(df, df_out)

class TestValidateOp():

    @pytest.mark.parametrize(
        "df",
        [
            ([]),
            ("pd.DataFrame()"),
            (pd.DataFrame()),
            (None),
            (
                pd.DataFrame({
                    "Name": ["a", "b"]
                })
            )
        ]
    )
    def test_validate_op_errors(self, df):
        """
            The provided inputs raise an error while executing
        """
        with pytest.raises(Exception) as _:
            df = obj.validate_op(df)

    @pytest.mark.parametrize(
        "df, df_out",
        [
            (
                pd.DataFrame({
                    "Name": ["trial 1"],
                    "Email": ["abc@domain.com"],
                    "Company_Phone": ["123-456-7890"]
                }),
                pd.DataFrame({
                    "Name": ["trial 1"],
                    "Email": ["abc@domain.com"],
                    "Company_Phone": ["123-456-7890"]
                })
            ),
            (
                pd.DataFrame({
                    "Name": ["trial 1"],
                    "Email": [""],
                    "Company_Phone": [""]
                }),
                pd.DataFrame({
                    "Name": ["trial 1"],
                    "Email": [""],
                    "Company_Phone": [""]
                }),
            ),
            (
                pd.DataFrame({
                    "Name": [""],
                    "Email": ["abc@domain.com"],
                    "Company_Phone": [""]
                }),
                pd.DataFrame({
                    "Name": [""],
                    "Email": ["abc@domain.com"],
                    "Company_Phone": [""]
                })
            ),
            (
                pd.DataFrame({
                    "Name": [""],
                    "Email": [""],
                    "Company_Phone": ["123-456-7890"]
                }),
                pd.DataFrame({
                    "Name": [""],
                    "Email": [""],
                    "Company_Phone": ["123-456-7890"]
                })
            ),
            (
                pd.DataFrame({
                    "Name": [None],
                    "Email": [None],
                    "Company_Phone": [None]
                }),
                pd.DataFrame(columns=["Name", "Email", "Company_Phone"])
            ),
            (
                pd.DataFrame({
                    "Name": [""],
                    "Email": [""],
                    "Company_Phone": [""]
                }),
                pd.DataFrame(columns=["Name", "Email", "Company_Phone"])
            ),
            (
                pd.DataFrame({
                    "Name": [""],
                    "Email": [""],
                    "Company_Phone": ["(+1)"]
                }),
                pd.DataFrame(columns=["Name", "Email", "Company_Phone"])
            ),
            (
                pd.DataFrame({
                    "Name": ["trial1-"],
                    "Email": [""],
                    "Company_Phone": [""]
                }),
                pd.DataFrame({
                    "Name": ["trial1"],
                    "Email": [""],
                    "Company_Phone": [""]
                }),
            ),
            (
                pd.DataFrame({
                    "Name": [None],
                    "Email": ["abc@domain.com"],
                    "Company_Phone": [""]
                }),
                pd.DataFrame({
                    "Name": [None],
                    "Email": ["abc@domain.com"],
                    "Company_Phone": [""]
                })
            ),
            (
                pd.DataFrame({
                    "Name": ["pqr"],
                    "Email": [None],
                    "Company_Phone": [""]
                }),
                pd.DataFrame({
                    "Name": ["pqr"],
                    "Email": [None],
                    "Company_Phone": [""]
                })
            ),
        ]
    )
    def test_validate_op_valid_entries(self, df, df_out):
        df = obj.validate_op(df)
        print(df)

        df_out = df_out.fillna("")
        assert_frame_equal(df, df_out)

class TestExtractData():

    @pytest.mark.parametrize(
        "dict_src, df_data",
        [
            ([], pd.DataFrame()),
            (pd.DataFrame(), pd.DataFrame()),
            ("events", []),
            ("events", "dataframe"),
            (None, pd.DataFrame()),
            ([], None),
            (pd.DataFrame(), None),
            (None, None),
            ("events", pd.DataFrame({"Company_b": ["DEF", "GHI"]})),
        ]
    )
    def test_extract_data_errors(self, dict_src, df_data):
        """
            The provided inputs raise an error while executing
        """
        with pytest.raises(Exception) as _:
            df_data = obj.extract_data(dict_src, df_data)

    @pytest.mark.parametrize(
        "dict_src, df_data",
        [
            ("contracts", pd.DataFrame()),
            ("PM", pd.DataFrame()),
            ("Renewel", pd.DataFrame()),
            ("Services", pd.DataFrame()),
        ]
    )
    def test_extract_data_same_op(self, dict_src, df_data):
        """
            The provided inputs raise an error while executing
        """
        df_out = obj.extract_data(dict_src, df_data)
        assert_frame_equal(df_out,df_data)

    def test_extract_data_config_not_load(self):
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
                        "Description": [" contact there Jhon Doe 118-0023-206"],
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
                        "Description": [" +91-123-456-7891 Jhon Doe 118-0023-206"]
                    }),
                    pd.DataFrame({
                        "Description": [" +91-123-456-7891 Jhon Doe 118-0023-206"],
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
    def test_extract_data_valid_entries(self, dict_src, df_data, df_out):
        df_data = obj.extract_data(
            "events", df_data
        )
        df_out = df_out.fillna("")
        assert_frame_equal(df_out, df_data)

class TestExceptionSrc():

    @pytest.mark.parametrize(
        "dict_src, df_data, dict_contact",
        [
            ([], pd.DataFrame(), {}),
            (pd.DataFrame(), pd.DataFrame(), {}),
            ("events", [], {}),
            ("events", "pd.DataFrame()", {}),
            ("events", pd.DataFrame(), "str"),
            ("events", pd.DataFrame(), pd.DataFrame()),
            (None, pd.DataFrame(), {}),
            ("events", None, {}),
            ("events", pd.DataFrame(), None),
        ]
    )
    def test_exception_src_raise_error(self, dict_src, df_data, dict_contact):
        with pytest.raises(Exception) as _:
            df_out, dict_updated = obj.exception_src(
                dict_src, df_data, dict_contact
            )

    @pytest.mark.parametrize(
        "dict_src, df_data, dict_contact",
        [
            ("events", pd.DataFrame(), {}),
            ("Renewal", pd.DataFrame(), {}),
            ("PM", pd.DataFrame(), {}),
            ("random", pd.DataFrame(), {})
        ]
    )
    def test_exception_src_same_op(self, dict_src, df_data, dict_contact):
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
    def test_exception_src_config_not_load(
            self, dict_src, df_data, dict_contact
    ):
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
