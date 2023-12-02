"""@file class_serial_number.py



@brief Convert range of SerialNumber to unique SerialNumber


@details
This module considers range of serial numbers provided from M2M database and
processes it to give unique serial numbers as an output.
Following submodules are processed
- Validate serial number
- Perform data cleaning operation on inserted serial number.
- Identify pattern
- Generate sequence of serial number
- Identify if the serial number is numeric / alphanumeric series
- Apply the logic range expansion to obtain unique serial number.

@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****

import re
import traceback
import pandas as pd
import sys
from decimal import Decimal #to deal with Decimal values (determined from the value of dictionary dict_mapping)
from utils import AppLogger

loggerObj = AppLogger(__name__)


# %%
#ensure_execution=True
count = 0

class SerialNumber:
    """
    This class implements logic for processing various types of range of serial
    numbers for converting it to unique serial numbers from m2m database.

    """

    def __init__(self, f_reset=False):
        """
        Function to initialize the csv file to read.

        :param f_reset: optional boolean value
        :type f_reset: Boolean datatype.
        :ref_data: Reads serial number csv file from data source
        :type ref_data: Default value is False.
        :raises Exception: None

        """

        ref_data = pd.DataFrame(columns=["SerialNumberOrg", "SerialNumber"])

        self.ref_data = ref_data.copy()

        self.dict_mapping = {}

    def check_var_size(self, local_vars, log):
        df_size = pd.DataFrame(columns=['Var', 'Size'])
        i = 0
        for var, obj in local_vars:
            df_size.loc[i, ] = [var, sys.getsizeof(obj)]
            i += 1
        
        df_size = df_size.sort_values(by=['Size'], ascending=False)
        if log:
            loggerObj.app_info(df_size.head(10))
            loggerObj.app_info(df_size.Size.sum())
        del df_size
        #return "success"

    def validate_srnum(self, ar_serialnum):
        """
        Perform validation of the serial numbers.
        Function validates if the serial number contains unaccepted keywords
        and then filters it as per the requirment.

        :param ar_serialnum: It specifies the serial number value to be
        processed.
        :type ar_serialnum: List of values.
        :raises Exception: Throws ValueError exception for Invalid values passed
        to function.
        :return df_data['f_valid']: Pandas series of Boolean whether its valid or not.
        :rtype: pandas Data Frame

        """

        current_step = "Serial number validation"
        # ar_serialnum = ['bcb-180-0557-1-2b-bus']
        try:
            df_data = pd.DataFrame(data={"SerialNumber": ar_serialnum})

            # Should not contain
            pat_invalid_self = [
                "fwt",
                "exp",
                "crat",
                "seis",  # Updated 03/07/2023
                "bcb",
                "bcms",
                "box",
                "cab",
                "com",
                "cratin",
                "ext",
                "floor",
                "freig",
                "inst",
                "jbox",
                "label",
                "line",
                "loadbk",
                "repo",
                "serv",
                "skirts",
                "spare",
                "su",
                "super",
                "test",
                "time",
                "trai",
                "trans",
                "tst",
                "warran",
                "nan",
                # Added section for Should not end in
                "bus",
                "combos",
                "ds",
                "ef",
                "ew",
                "fl",
                "flsk",
                "fs",
                "fsk",
                "jb",
                "lg",
                "lll",
                "misc",
                "nin",
                "pm",
                "rpp",
                "sk",
                "skirt",
                "skt",
                "spg",
                "supk",
                "tap",
                "tm",
                "tspk",
            ]

            pat_invalid_self = "(" + "|".join(pat_invalid_self) + ")"
            df_data.loc[:, "f_valid_content"] = df_data["SerialNumber"].apply(
                lambda x: re.search(pat_invalid_self, str(x)) == None
            )
            del pat_invalid_self

            # Should not end in
            pat_invalid_self = [
                "bus",
                "combos",
                "ds",
                "ef",
                "ew",
                "fl",
                "flsk",
                "fs",
                "fsk",
                "jb",
                "lg",
                "lll",
                "misc",
                "nin",
                "pm",
                "rpp",
                "sk",
                "skirt",
                "skt",
                "spg",
                "supk",
                "tap",
                "tm",
                "tspk",
            ]
            pat_invalid_self = "(" + "$|".join(pat_invalid_self) + "$)"

            df_data.loc[:, "f_valid_end"] = df_data["SerialNumber"].apply(
                lambda x: re.search(pat_invalid_self, str(x)) == None
            )

            df_data["f_valid"] = df_data["f_valid_end"] & df_data["f_valid_content"]

            loggerObj.app_debug(current_step)

        except Exception as e:
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_data[["f_valid"]]

    def prep_srnum(self, df_input):
        """
        Perform data cleaning operation for inserted serial numbers.

        :param df_input: Serial number is passed to the function for replacing
        characters as per the requirements. Replace the unknown characters with
        the set of required characters.
        :type df_input: Pandas DF.
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return df_input[col]: Pandas series with serial number.
        :rtype: Pandas Data Frame

        """

        current_step = "Serial number cleaning"

        try:
            col = "SerialNumberOrg"
            # PreProcess Serial Numbers
            pat_punc = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~ "
            df_input[col] = df_input[col].str.lstrip(pat_punc).str.rstrip(pat_punc)
            df_input[col] = df_input[col].str.replace(" ", "")
            df_input[col] = df_input[col].str.replace(r"&", r"-")

            # Change made: 2023-27-9 Expand AB suffix for specific cases
            # Change made for correct expansion
            df_input[col] = df_input.apply(lambda row: self.modify_sr_num(row), axis=1)
            loggerObj.app_info(f"the content of df_input is {df_input}")
            loggerObj.app_debug(current_step)

        except Exception as e:
            loggerObj.app_info(f"The error message generated is {str(e)}")
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise Exception from e
        return df_input[col]

    def get_serialnumber(
        self, ar_serialnum, ar_installsize, ar_key_serial, data_type="m2m"
    ):
        """
        Function accepts and initializes the parameters for serial number
        and m2m data type. It performs data initialization and bifurcates
        data type results into df_out and df_couldnot by calling unknown_range
        type function.

        :param ar_serialnum: It specifies the serial number value to be
        processed.
        :type ar_serialnum: List of values.
        :param ar_installsize:It denotes the size of the array values as
        specified in ar_serialnum
        :type ar_installsize:List of values
        :param data_type:The default is 'm2m'.
        :type data_type:String type, optional
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return df_out, df_could_not: Pandas series with bifurcated serial
        number as list of valid and invalid segregated unique serial numbers.
        :rtype: Pandas Data Frame

        """
        
        current_step = "Serial number initialization and segregation"
        loggerObj.app_info(f"The value of ar_serialnum is {ar_serialnum}")
        loggerObj.app_info(f"The value of ar_installsize is {ar_installsize}")
        loggerObj.app_info(f"The value of ar_installsize is {ar_key_serial}")
        
        try:
            self.data_type = data_type

            df_org = pd.DataFrame(
                data={
                    "SerialNumberOrg": ar_serialnum,
                    "InstallSize": ar_installsize,
                    "KeySerial": ar_key_serial,
                }
            )

            loggerObj.app_info(f"The content of df_org is {df_org}")
            #loggerObj.app_info(f"The column names of df_org is {df_org.columns}")
            df_org["known_sr_num"] = False
            loggerObj.app_info(f"The content of df_org after adding new column known_sr_num is {df_org}")
            #loggerObj.app_info(f"The column names of df_org is {df_org.columns}")
            
            # UnKnown ranges
            df_subset = df_org.loc[
                df_org["known_sr_num"] == False,
                ["SerialNumberOrg", "InstallSize", "KeySerial"],
            ]
            #loggerObj.app_info("The objects along with their memory consumption in class_serial_number.py are") 
            #self.check_var_size(list(locals().items()), log=True)
            loggerObj.app_info("Number of rows in df_org in function get_serialnumber in class_serial_number.py are {0} Number of rows in df_subset is {1}".format(len(df_org), len(df_subset)))

            loggerObj.app_info(f"The content of data frame df_subset is {df_subset}")
            df_out_unknown, df_could_not = self.unknown_range(df_subset)
            del df_subset
            loggerObj.app_info("Finished calling unknown_range method defined inside class_serial_number.py")
            loggerObj.app_info("Before clubbing df_could_not and df_out_unknown")
            # Club Data
            if False:
                #df_out = pd.concat([df_out_known, df_out_unknown])
                df_out = pd.concat([df_could_not, df_out_unknown])
            else:
                loggerObj.app_info("Inside else")
                df_out = df_out_unknown.copy()
                del df_out_unknown
            loggerObj.app_debug(current_step)
            loggerObj.app_info("Now returning from get_serialnumber method defined inside class_serial_number.py")
        except Exception as e:
            loggerObj.app_info(f"The error message generated is {str(e)}")
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_out, df_could_not

    def known_range(self, df_input):  # pragma: no cover
        """
        Function contains known dataframe values which have been processed
        earlier by the code. It will help in filtering the serial number types
        processed earlier.
        Note: Method not implemented currently.
        # pragma: no cover

        :param df_input: Serial number is passed to the function.
        It will record the earlier processed serial numbers which may be
        validated against serial number data inserted for further processing.
        :type df_input: Pandas Dataframe.
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return df_out_known: Returns the dataframe of values which are known.
        :rtype:  Pandas Dataframe

        """

        current_step = "Known range of serial numbers"

        try:
            ref_data = self.ref_data

            ls_cols = ["SerialNumberOrg", "SerialNumber"]
            df_out_known = df_input.merge(
                ref_data[ls_cols], on="SerialNumberOrg", how="right"
            )
            loggerObj.app_info(current_step)  # pragma: no cover

        except Exception as e:
            df_out_known = pd.DataFrame(columns=["SerialNumber", "SerialNumberOrg"])
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_out_known  # pragma: no cover

    def unknown_range(self, df_input):
        """
        Function processes the range of serial numbers which are unknown to the
        code or which have not been processed earlier.

        :param df_input: Serial number is passed to the function.
        :type df_input: Pandas Dataframe.
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return df_out_unknown,could_not: List of values processed by the
        function,  List of values which could not be processed by the
        .function
        :rtype:  Pandas Dataframe

        """
        current_step = "Unknown range of serial numbers"
        loggerObj.app_info(f"Inside unknown range method with df_input as {df_input}")
        #global ensure_execution
        try:
            ls_results = [
                "f_analyze",
                "type",
                "ix_beg",
                "ix_end",
                "pre_fix",
                "post_fix",
            ]

            # Clean Serial Number
            loggerObj.app_info("Calling prep_srnum in class_serial_number.py")
            #loggerObj.app_info("The objects along with their memory consumption in class_serial_number.py are")
            #self.check_var_size(list(locals().items()), log=True)

            loggerObj.app_info(f"Number of rows in dataframe df_input inside function unknown_range in class_serial_number.py before calling prep_srnum in class_serial_number.py are {len(df_input)}")
            df_input["SerialNumber"] = self.prep_srnum(df_input)
            loggerObj.app_info(f"After Calling prep_srnum in class_serial_number.py the content of df_input is {df_input}")
            loggerObj.app_info(f"Number of rows in dataframe df_input inside function unknown_range in class_serial_number.py after calling prep_srnum in class_serial_number.py are {len(df_input)}")
            # Identify Type of Sequence:
            cols = ["SerialNumberOrg", "InstallSize", "KeySerial"]

            #loggerObj.app_info("The objects along with their memory consumption in class_serial_number.py are")
            #self.check_var_size(list(locals().items()), log=True)
            loggerObj.app_info("Calling identify_seq_type in class_serial_number.py")
            loggerObj.app_info(f"Number of rows in dataframe df_input inside function unknown_range in class_serial_number.py before calling identify_seq_type in class_serial_number.py are {len(df_input)}")
            df_input["out"] = df_input[cols].apply(
                lambda x: self.identify_seq_type(x), axis=1
            )
            loggerObj.app_info("Finished Calling identify_seq_type in class_serial_number.py")
            loggerObj.app_info(f"Number of rows in dataframe df_input inside function unknown_range in class_serial_number.py after calling identify_seq_type in class_serial_number.py are {len(df_input)}")
            ix = 0
            for col in ls_results:
                df_input[col] = df_input["out"].apply(lambda x: x[ix])
                ix += 1
            
            loggerObj.app_info(f"Number of rows in dataframe df_input inside function unknown_range in class_serial_number.py before calling generate_seq in class_serial_number.py are {len(df_input)}")
            loggerObj.app_info("Calling generate_seq method defined in class_serial_number.py")
            # Generate Sequence
            # if ensure_execution:
            #     df_input = df_input.iloc[27390:27400]
            #     #df_input = df_input[df_input['SerialNumberOrg'] != '110-0333-A-B']
            #     ensure_execution=False
            
            #loggerObj.app_info(f"Now calling generate_seq function defined in class_serial_number.py with number of rows = {df_input.index.stop}")
            loggerObj.app_info(f"Now calling generate_seq function defined in class_serial_number.py with number of rows = {len(df_input)}")
            ls_seq_out_unknown = df_input[
                ["out", "SerialNumberOrg", "InstallSize", "KeySerial"]
            ].apply(lambda x: self.generate_seq(x[0], x[1], x[2], x[3]), axis=1)
            #loggerObj.app_info(f"Number of rows processed by calling generate_seq function defined in class_serial_number.py are = {df_input.index.stop}")
            loggerObj.app_info(f"Number of rows processed by calling generate_seq function defined in class_serial_number.py are = {len(df_input)}")
            loggerObj.app_info("Completed execution of generate_seq in the class class_serial_number.py")
            df_out_unknown = pd.concat(ls_seq_out_unknown.tolist())

            could_not = df_input.loc[df_input["f_analyze"] == False, :]
            loggerObj.app_debug(current_step)

        except Exception as e:
            loggerObj.app_info(str(e))
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise Exception from e

        loggerObj.app_info("Now returning from function unknown_range defined in class_serial_number.py")
        return df_out_unknown, could_not

    def generate_seq_list(self, dict_data):
        """
        Function generates the sequence of characters for the inserted serial
        number with list type.
        It is a sub-module under generate_seq function.

        :param dict_data:Inputs a dictionary containing parameters values. 
        :type dict_data: Dictionary.
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return rge_sr_num: Generated values for serial number series.
        :rtype:  List

        """

        current_step = "Generating list of sequence of serial numbers"

        try:
            temp_sr = str.split(dict_data["ix_end"], ",")
            rge_sr_num = []
            for sr in temp_sr:
                if "-" not in sr:
                    rge_sr_num = rge_sr_num + [int(sr)]
                else:
                    split_sr = sr.split("-")
                    rge_sr_num = rge_sr_num + list(
                        range(int(split_sr[0]), int(split_sr[1]) + 1)
                    )

                loggerObj.app_debug(current_step)
        except Exception as e:
            loggerObj.app_info(f"Error message generated in generate_seq_list is {str(e)}")
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise e

        return rge_sr_num

    def generate_seq(self, out, sr_num, size, key_serial):
        """
        Function generates the sequence of characters for the inserted serial
        numbers.

        :param out: Contains data type of serial number to be processed
        along with the prefix and postfix data.
        :type out: List of values.
        :param sr_num: Serial number to be processed.
        :type sr_num: String.
        :param size:  Range of serial number.
        :type size: Integer
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return df_out: Returns dataframe of resulting values.
        :rtype: Pandas Dataframe

        """
        # out = [True] + list(dict_out.values())

        df_out = pd.DataFrame(columns=["SerialNumberOrg", "SerialNumber"])
        try:
            global count
        
            #Fix for cannot access local variable 'rge_sr_num' where it is not associated with a value
            rge_sr_num = []
            ls_out_n = ["f_analyze", "type", "ix_beg", "ix_end", "pre_fix", "post_fix"]
            dict_data = dict(zip(ls_out_n, out))
            loggerObj.app_info(f"The key and values of the dictionary dict_data are {dict_data.keys()} and {dict_data.values()} and Index number is {count}")
            count = count + 1

            if dict_data["type"] == "list":
                rge_sr_num = self.generate_seq_list(dict_data)

                if len(rge_sr_num) < size:
                    temp_sr = str.split(dict_data["pre_fix"], "-")
                    dict_data["pre_fix"] = "-".join(temp_sr[:-1])
                    dict_data["ix_end"] = temp_sr[-1] + "-" + dict_data["ix_end"]
                    rge_sr_num = self.generate_seq_list(dict_data)

            if dict_data["type"] in ["num", "num_count"]:
                #To handle invalid serial number for e.g. 213-327-1247-8435663127
                if int(dict_data["ix_end"]) - int(dict_data["ix_beg"]) > 150:
                    loggerObj.app_info(f"Discarding serial number {sr_num}")
                    rge_sr_num = []
                else:
                    rge_sr_num = range(
                        int(dict_data["ix_beg"]), int(dict_data["ix_end"]) + 1
                    )

            if dict_data["type"] == "alpha":
                filter_size = 100
                count_sr = (
                    self.identify_index(dict_data["ix_end"])
                    - self.identify_index(dict_data["ix_beg"])
                ) + 1
                if (
                    count_sr < filter_size
                ):  # BugFix: Consider the expansion where count is not greater than size
                    loggerObj.app_info(f"The serial number for which letter_range function is called is {sr_num}")
                    rge_sr_num = self.letter_range(dict_data["ix_beg"], count_sr)

            ls_srnum = [
                (dict_data["pre_fix"] + str(ix_sr) + dict_data["post_fix"])
                for ix_sr in rge_sr_num
            ]
            #loggerObj.app_debug(current_step)

        except Exception as e:
            loggerObj.app_info(f"The serial number for which the issue has been reported is {sr_num}")
            loggerObj.app_info(str(e))
            ls_srnum = []

        if (
            (len(ls_srnum) > size)  # size
            and (len(ls_srnum) > 100)
            and (self.data_type == "m2m")
        ):
            loggerObj.app_debug(f"{sr_num}: {len(ls_srnum)} > {size}", 1)
            ls_srnum = []
        elif (
            (len(ls_srnum) > size)  # size
            and (len(ls_srnum) > 150)
            and (self.data_type == "contract")
        ):
            loggerObj.app_debug(f"{sr_num}: {len(ls_srnum)} > {size}", 1)
            ls_srnum = []

        df_out["SerialNumber"] = ls_srnum
        df_out["SerialNumberOrg"] = [sr_num] * len(ls_srnum)
        df_out["KeySerial"] = key_serial

        #loggerObj.app_info("The objects along with their memory consumption in generate_seq in class_serial_number.py are")
        
        #self.check_var_size(list(locals().items()), log=True)
        loggerObj.app_info("Reached end of generate_seq method in class_serial_number.py")
        return df_out

    def identify_seq_type(self, vals):
        """
        Function identifies the valid sequnce type for the serial number
        data inserted. It categorizes the data based on numeric
        alphanumeric types.

        :param vals:List containing serial number and range of generating
        sequence of the serial number.
        :type vals: Python List.
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return : Returns a List containing type of sequence as
        alphanumeric or numeric data type along with the prefix and postfix
        values of the data from serial number..
        :rtype:  Python List

        """

        current_step = "Identifying sequence of serial numbers"

        try:
            #loggerObj.app_info(f"The contents of dict_mapping are {self.dict_mapping}")
            # vals = ['12017004-51-59,61', 10]       110-1900-12,14,17,19
            sr_num = vals[0]
            install_size = vals[1]
            loggerObj.app_debug(sr_num)

            f_analyze = True
            dict_out = {
                "type": "",
                "ix_beg": "",
                "ix_end": "",
                "pre_fix": "",
                "post_fix": "",
            }

            sr_num = str.replace(str(sr_num), "/", "-")
            sr_num = str.replace(str(sr_num), "--", "-")
            split_sr_num = str.split(str(sr_num), "-")
            pre_fix = post_fix = ix_beg = ix_end = ''

            # Type = num_count
            # Example : SrNum : 110-115; InstallSize = 10
            # Here index of unique serial numbers are not provided.
            # Therefore, sequence with length of InstallSize starting from 1
            # should be created

            #A conversion to float type is performed/incorporated in this python file to prevent the execution failing due to the error message - unsupported operand type(s) for +: 'decimal.Decimal' and 'float'
            if (len(split_sr_num) == 2) and ("," not in sr_num):
                if sr_num in self.dict_mapping:
                    #loggerObj.app_info("Inside if started")
                    ix_beg = float(self.dict_mapping[sr_num]) +float(1)
                    ix_end = float(self.dict_mapping[sr_num]) + float(install_size)
                    self.dict_mapping[sr_num] = ix_end
                    #loggerObj.app_info("Inside if completed")
                else:
                    #loggerObj.app_info("Inside else started")
                    self.dict_mapping[sr_num] = install_size
                    ix_beg = 1
                    ix_end = install_size
                    #loggerObj.app_info("Inside else completed")
                dict_out["type"] = "num_count"
                dict_out["ix_beg"] = ix_beg
                dict_out["ix_end"] = ix_end
                dict_out["pre_fix"] = sr_num + "-"
                ls_out = [True] + list(dict_out.values())
                #loggerObj.app_info(f"The value of ls_out is {ls_out}")
                return ls_out

            # If there are only one component in serial number; then its not a valid
            # serial number. Therefore, f_analyze = False
            if len(split_sr_num) < 2:
                f_analyze = False
                ls_out = [f_analyze] + list(dict_out.values())
                return ls_out

            if ("," in split_sr_num[-2]) and (len(split_sr_num[-2].split(",")) == 2):
                first_val = split_sr_num[-2].split(",")[0]
                loggerObj.app_info(f"The serial number is {sr_num}")
                second_val = split_sr_num[-2].split(",")[1]
                split_sr_num.pop(-2)
                split_sr_num.insert(1, second_val)
                split_sr_num.insert(1, first_val)
                sr_num = sr_num.replace(",", "-")

            ix_beg, ix_end = split_sr_num[-2], split_sr_num[-1]
            if len(split_sr_num[:-2]) > 0:
                pre_fix = "-".join(split_sr_num[:-2]) + "-"
            else:
                pre_fix = ""

            try:
                if "," in sr_num:
                    dict_out["type"] = "list"
                    # type = list
                    # Example : [118-110-1,2,3]
                    split_sr_num = str.split(str(sr_num), ",")
                    temp_str = str.split(str(split_sr_num[0]), "-")

                    if split_sr_num[0].count("-") in [2, 3]:
                        pre_fix = "-".join(temp_str[:2])
                        ix_end = "-".join(temp_str[2:])
                        ix_end = ",".join([ix_end] + split_sr_num[1:])

                        ix_beg = ""
                    elif split_sr_num[0].count("-") in [1]:
                        pre_fix = temp_str[0]
                        ix_end = ",".join([temp_str[1]] + split_sr_num[1:])
                    else:
                        f_analyze = False

                elif ix_beg.isalpha() & ix_end.isalpha():
                    dict_out["type"] = "alpha"
                elif ix_beg.isdigit() & ix_end.isdigit():
                    dict_out["type"] = "num"

                elif ix_beg.isdigit() & ix_end.isalnum():
                    dict_out["type"] = "num"

                    loc_split = [
                        ix for ix in range(len(ix_end)) if (ix_end[ix].isdigit())
                    ]
                    dict_out["post_fix"] = ix_end[max(loc_split) + 1 :]
                    ix_end = ix_end[: max(loc_split) + 1]

                elif ix_beg.isalnum() & ix_end.isalpha():
                    dict_out["type"] = "alpha"
                    loc_split = [
                        ix for ix in range(len(ix_beg)) if (ix_beg[ix].isdigit())
                    ]
                    pre_fix = pre_fix + ix_beg[: max(loc_split) + 1] + "-"
                    ix_beg = ix_beg[max(loc_split) + 1 :]

                elif ix_beg.isalnum() & ix_end.isalnum():
                    loc_split_end = [
                        ix for ix in range(len(ix_end)) if (ix_end[ix].isdigit())
                    ]
                    loc_split_beg = [
                        ix for ix in range(len(ix_beg)) if (ix_beg[ix].isdigit())
                    ]

                    if (
                        ix_end[max(loc_split_end) + 1 :]
                        == ix_beg[max(loc_split_beg) + 1 :]
                    ):
                        dict_out["type"] = "num"
                        dict_out["post_fix"] = ix_end[max(loc_split_end) + 1 :]

                        ix_end = ix_end[: max(loc_split_end) + 1]
                        ix_beg = ix_beg[: max(loc_split_beg) + 1]
                    else:
                        f_analyze = False
                else:
                    f_analyze = False
            except Exception as e:
                loggerObj.app_info(f"Error message inside inner except block is {str(e)}")
                loggerObj.app_info(f"The serial number for which the max() error is reported is {sr_num}")
                loggerObj.app_info(f"Error message generated is {str(e)}")
                return [False] + list(dict_out.values())

            if f_analyze:
                # if ix_end.isnumeric() and sr_num in self.dict_mapping:
                if (
                    ix_beg.isnumeric()
                    and ix_end.isnumeric()
                    and sr_num in self.dict_mapping
                ):
                    temp = int(ix_end) - int(ix_beg)
                    ix_beg = float(self.dict_mapping[sr_num]) + float(1)
                    ix_end = ix_beg + temp
                    self.dict_mapping[sr_num] = int(ix_end)
                else:
                    if ix_end.isnumeric():
                        self.dict_mapping[sr_num] = int(ix_end)

                dict_out["pre_fix"] = pre_fix
                dict_out["ix_beg"] = ix_beg
                dict_out["ix_end"] = ix_end

            loggerObj.app_debug(current_step)

        except Exception as e:
            loggerObj.app_info(f"Error message in outer except block is {str(e)}")
            loggerObj.app_info(f"Error message in outer except block is due to the serial number {sr_num}")
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise e

        #loggerObj.app_info("The objects along with their memory consumption in class_serial_number.py are")
        #self.check_var_size(list(locals().items()), log=True)
        return [f_analyze] + list(dict_out.values())

    def letter_range(self, seq_, size):
        """
        This function generates a range of letters from the given input params.
        If sequence provided as 'a' with a size of 17, the expected output of
        function is from 'a,b,c,d.....q'.

        :param seq_: Inputs a character value to be incremented.
        :type seq_: Character.
        :param size: Numeric digits / Count by which the character value needs
        to be incremented by.
        :type size: Integer.
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return ls_sr_num: Range of Characters.
        :rtype:  Python List

        """

        current_step = "Range of letters"

        try:
            pwr = len(seq_)
            ix = self.identify_index(seq_)

            ls_sr_num = []
            for ix_sr in range(ix, ix + size):
                # ix_sr = list(range(ix, ix+size))[0]
                srnum = self.convert_index(ix_sr, pwr)
                ls_sr_num.append(srnum)
            loggerObj.app_debug(current_step)

        except Exception as e:
            loggerObj.app_info(str(e))
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise Exception from e

        return ls_sr_num

    def identify_index(self, seq_):
        """
        Function identifies index value position wrt character input.
        Eg. For character input 'q', the index value output will be 17.

        :param seq_: Inputs a character value to be incremented. Ex-'a'
        :type seq_: Character.
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return val_total: Returns integer value depicting count of alphabets
        required to be incremented.
        :rtype:  Integer

        """

        current_step = "Identify Index value"

        try:
            val_total = 0
            for pos in list(range(len(seq_))):
                # pos = 1
                ix_aplha = ord(seq_[pos]) - 96
                pwr_alpha = len(seq_) - pos - 1
                val_aplha = (26**pwr_alpha) * ix_aplha
                val_total = val_total + val_aplha
            loggerObj.app_debug(current_step)

        except Exception as e:
            loggerObj.app_info(str(e))
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise Exception from e

        return val_total

    def convert_index(self, ix_n, pwr):
        """
        Function increments character value.
        Eg. it will return total_txt as a series of a to q for ix_n

        :param ix_n: Incrementing counter value passed from identify_index()
        :type ix_n: Integer.
        :param pwr: Incrementing counter value passed from identify_index()
        :type pwr: Integer.
        :raises Exception: Throws ValueError exception for Invalid values
        passed to function.
        :return total_txt: Incremented Value.
        :rtype:  Character

        """

        current_step = "Convert Index value"

        try:
            loggerObj.app_info(f"Inside the function convert_index with arguments {ix_n} and {pwr}")
            total_txt = ""
            for loc in list(range(pwr, 0, -1)):
                # loc = list(range(pwr, 0, -1))[1]
                val = ix_n // (26 ** (loc - 1))
                total_txt = total_txt + chr(96 + val)
                ix_n = ix_n % (26 ** (loc - 1))
            #loggerObj.app_info(current_step)

        except Exception as e:
            loggerObj.app_info(str(e))
            loggerObj.app_fail(current_step, f"{traceback.print_exc()}")
            raise Exception from e

        return total_txt

    def modify_sr_num(self, row):
        """
        Method to expand 2 alphabet strings with hyphen seperator
        @param row: row entry containing SerialNumber, InstallSize, KeySerial
        @return: updated serial number string
        """
        serial_num = row.SerialNumberOrg
        size = row.InstallSize
        serial_num_parts = serial_num.split("-")
        if (
            (
                (len(serial_num_parts) <= 3)
                or  # Original Serial Number
                # with 2 hyphen e.g. 110-014-0AB
                (len(serial_num_parts) == 4 and len(serial_num_parts[-1]) == 2)
                # Updated Serial Number with 3 hyphens
                # but last part contains only 2 alphabets i.e. after expansion it will
                # have 3 hyphens. For. e.g. X-Y-Z-ab expands into X-Y-Z-a and Y-Y-Z-b.
                # Haven't come across this case yet but handled it considering future
                # possiblity, for e.g. it would be true for e.g. 110-014-0-AB but false
                # for e.g. 110-014-0-1AB
            )
            and len(serial_num_parts) > 1
            and size == 2
        ):
            last_part = serial_num_parts[-1]
            if len(last_part) >= 2:
                last_part_prefix = last_part[:-2]
                char1 = last_part[-2:-1]
                char2 = last_part[-1:]

                if (
                    # Check if the 2 characters are consecutive alphabets
                    char1.isalpha()
                    and char2.isalpha()
                    and (ord(char2) - ord(char1)) == 1
                ):
                    updated_sr_num = "-".join(serial_num_parts[:-1])
                    updated_sr_num = updated_sr_num + "-"
                    hyphen_sep_alpha = char1 + "-" + char2
                    if last_part_prefix != "":
                        return (
                            updated_sr_num + last_part_prefix + "-" + hyphen_sep_alpha
                        )
                    return updated_sr_num + hyphen_sep_alpha

        return serial_num


# %%
if __name__ == "__main__":  # pragma: no cover
    sr_num = SerialNumber(f_reset=True)
    # ar_serialnum = ['180-05578a-q', '180-0557-1-2b',
    #                 '180-0557-1-2', '560-0152-4-8']
    #
    # ar_installsize = [17, 2, 2, 4]
    # 11100067
    # ar_serialnum = ['110-0466', '442-0002-7a-12a', '442-0002-7a-12a','bcb-180-0557-1-2b-bus']
    ar_serialnum = ["110-014-0AB"]
    # ar_serialnum = ['112-0058-1-7','112-0058-6-9,11-12']
    # ar_serialnum = ['110-0126AB']
    # ar_serialnum = ['118-110-1,2,3']   # Need to resolve
    ar_installsize = [2]

    df_out_srs, df_out_couldnot = sr_num.get_serialnumber(
        ar_serialnum, ar_installsize, "1"
    )
    print("The df Out data is ", df_out_srs)
    # df_data = pd.read_csv('./data/SerialNumber.csv')
# %%
