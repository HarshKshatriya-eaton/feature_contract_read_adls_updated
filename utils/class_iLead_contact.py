# -*- coding: utf-8 -*-
"""
Created on Mon Nov 15 12:39:08 2021

@author: E9780837
"""

# %% ***** Load Libraries *****
import logging
import re
from datetime import datetime

import numpy as np
import pandas as pd


# %% ***** Define Class : iLead Contacts *****


class ilead_contact:
    """Functions involved in generating contacts file."""

    def __init__(self):
        self.TH_MIN_INSTALL_DATE = pd.to_datetime("1/1/2000")

    def get_city(self, zc, zcdb):
        """
        Get city name based on Zip Code.

        Parameters
        ----------
        zc : string. ZipCode from address.
        zcdb : pandas DataFrame. Mapping Zip Code to following columns
            - City, State, Country
            - Latitude, Longitude

        Returns
        -------
        city : string. City Name.
        """
        zc = zc.split("-")[0]
        try:
            zc = int(zc)
            try:
                city = zcdb[zc].city
            except:
                city = ""
        except:
            city = ""
        return city

    def clean_entries_validity(self, dataset, col_name, action):
        """
        Pre-Process Data.

        Parameters
        ----------
        dataset : pandas dataframe.
            Single column dataframe with data to be processed.
        col_name : string. Column to be processed.
        action : string. Action to be taken on columns of interest. Allowed
        actions are listed below:
                - Clean
                - validity

        Returns
        -------
        Pandas data frame with processed data.
        """
        logging.info(
            "On line 77 of class_ilead_contact function- clean_entries_validity"
        )
        data = dataset.copy()
        del dataset

        data = data.rename(columns={data.columns[0]: "text_org"})
        try:
            if action == "clean":
                data["text"] = data["text_org"].copy()

                if col_name == "City":
                    data["is_alphanum"] = data["text"].str.contains(
                        "\\d", regex=True)
                    data.loc[data["is_alphanum"], "text"] = ""
                    data["output"] = data["text"].copy()

                elif col_name == "Zipcode":
                    th_valid = 2

                    # string length
                    data["text_len"] = data["text"].str.len()
                    data.loc[data["text_len"] < th_valid, "text"] = ""

                    # Remove extra Info
                    data["text"] = data["text"].apply(
                        lambda x: x.split("-")[0] if ("-" in str(x)) else x
                    )

                    # character length: 4 to 5
                    data["text"] = pd.to_numeric(data["text"], errors="coerce")
                    data["text"] = data["text"].fillna("")

                    data["text"] = data["text"].apply(
                        lambda x: x if x == "" else format(int(x), "05d")
                    )

                    data["output"] = data["text"].copy()

                elif col_name == "Company_Phone":

                    # string length
                    TH_STR_LEN = 9

                    # Clean Ph Num
                    data["text"] = data["text"].str.lower()
                    data["text"] = data["text"].apply(
                        lambda x: x.replace("--", "-"))

                    # Remove special characters
                    data["text_clean"] = data["text"].apply(
                        lambda x: re.sub("[\W_]+", "", str(x))
                    )

                    ls_non_phone = ["xxxxx", "9999999999",
                                    "1111111111", "0000000000"]
                    data["text_clean"] = data["text_clean"].apply(
                        lambda x: "" if (str(x) in ls_non_phone) else x
                    )
                    logging.info(
                        "On line 130, Company Phone- class_ilead_contact")
                    data["text_clean"] = data["text_clean"].apply(
                        lambda x: x if len(x) == 0 else x[:-1] if x[-1] == "-" else x
                    )

                    # Clean Phone Number
                    ls_txt = ["tel", "cell"]
                    for txt in ls_txt:
                        data["text_clean"] = data["text_clean"].str.replace(
                            txt, "", regex=True
                        )
                    logging.info(
                        "On line 140, Company Phone- class_ilead_contact")
                    # Get the string length
                    data["text_len"] = data["text_clean"].str.len()
                    data["text_org_len"] = data["text_org"].str.len()
                    logging.info(
                        "On line 144, Company Phone- class_ilead_contact")
                    # Format Phone Number
                    ls_txt = ["text_clean", "text", "text_len"]

                    data["text_new"] = data["text"]
                    logging.info(
                        "On line 155, Company Phone- class_ilead_contact")
                    data.loc[data["text_len"] <= TH_STR_LEN, "text_new"] = ""
                    data["output"] = data["text_new"].copy()
                    data["output"] = data["output"].apply(
                        lambda x: x
                        if len(x) == 0
                        else x[1:]
                        if x[0] == "-"
                        else x[2:]
                        if x[:2] == "1-"
                        else x
                    )
                    logging.info(
                        "On line 162, Company Phone- class_ilead_contact")
                    data["output"] = data["output"].apply(
                        lambda x: x if len(x) == 0 else x[:-1] if x[-1] == "-" else x
                    )
            elif action == "validity":
                data["text"] = data["text_org"].replace(np.nan, "", regex=True)
                data["text"] = data["text"].apply(
                    lambda x: re.sub("[\W_]+", "", str(x))
                )
                data["text"] = data["text"].str.upper()
                if col_name == "Email":
                    ls_not_ups = ["INVALID", "BLANK",
                                  "UNKNOWN", "NOT", "XXX"]  # No
                    TH_STR_LEN = 3
                elif col_name == "Name":
                    TH_STR_LEN = 1
                elif col_name == "Company_Phone":
                    TH_STR_LEN = 5
                elif col_name == "Address1":
                    TH_STR_LEN = 2
                elif col_name == "Address2":
                    TH_STR_LEN = 2
                # Flag baed on string length
                data["flag_Valid"] = data["text"].str.len()
                data["flag_Valid"] = data["flag_Valid"] > TH_STR_LEN
                # Flag baed on string
                if "ls_not_ups" in locals():
                    non_ups_regex = "|".join(ls_not_ups)
                    data["flag_text"] = ~data["text"].str.contains(
                        non_ups_regex, regex=True
                    )
                else:
                    data["flag_text"] = True
                data["output"] = data["flag_text"] & data["flag_Valid"]
        except Exception as e:
            logging.info(f"On line 189, Error: {e}")
            return e

        return data["output"]

    def clean_date(self, dataset, ls_date_formats=[]):
        """
        Decode dates based on most popular date format.

        Parameters
        ----------
        dataset : pandas dataframe with dathe column.
        ls_date_formats : list, optional. List of possible date formats
        used for decoding.

        Returns
        -------
        out_df : pandas Data Frame. Decoded date column. Dates wghich cannt be
        decoded withh return NA for that record.

        """
        empty_date = datetime(1800, 1, 1)

        temp_df = dataset.copy()
        del dataset

        # Rename columns
        temp_df = temp_df.rename(columns={temp_df.columns[0]: "text_org"})
        temp_df["text_org"] = temp_df["text_org"].astype(str)

        # Clean Dates
        temp_df["DateClean"] = temp_df["text_org"].copy()
        temp_df["DateClean"] = temp_df["DateClean"].str.strip()
        temp_df["DateClean"] = temp_df["DateClean"].apply(
            lambda x: x.replace("//", "-")
        )
        temp_df["DateClean"] = temp_df["DateClean"].apply(
            lambda x: x.replace("/", "-"))
        temp_df["DateClean_DT"] = temp_df["DateClean"].copy()
        temp_df["DateClean"] = temp_df["DateClean"].apply(
            lambda x: x.replace(" ", "-"))

        # Initialize
        if len(ls_date_formats) == 0:
            ls_date_formats = [
                "%m-%d-%Y %H:%M",
                "%d-%m-%y",
                "%d-%m-%Y",
                "%m-%d-%y",
                "%m-%d-%Y",
                "%b-%y",
                "%b-%Y",
                "%m-%Y",
                "%m-%y",
            ]
        df_decision = pd.DataFrame({"DateFormat": ls_date_formats})
        df_decision["Count"] = 0
        df_decision["ColName"] = ""

        # Try all date formats
        for ix_form in range(len(ls_date_formats)):
            col_name = f"DF_{ix_form}"
            form = ls_date_formats[ix_form]
            if form == "%m-%d-%Y %H:%M":
                temp_df.loc[:, col_name] = temp_df["DateClean_DT"].apply(
                    lambda x: x.split(" ")[0]
                )
                flag = temp_df[col_name].str.len()
                temp_df.loc[flag <= 6, col_name] = ""

                formN = form.split(" ")[0]
                temp_df.loc[:, col_name] = pd.to_datetime(
                    temp_df[col_name], format=formN, errors="coerce"
                )
            else:
                temp_df.loc[:, col_name] = pd.to_datetime(
                    temp_df["DateClean"], format=form, errors="coerce"
                )

            n_format = pd.notna(temp_df[col_name]).sum()
            df_decision.loc[df_decision["DateFormat"]
                            == form, "Count"] = n_format
            df_decision.loc[df_decision["DateFormat"]
                            == form, "ColName"] = col_name

        # Prioratize Date format (based on suucess rate)
        df_decision = df_decision.loc[df_decision["Count"] > 0, :].reset_index(
            drop=True
        )
        if df_decision.shape[0] > 0:
            df_decision = df_decision.sort_values(
                by=["Count"], ascending=False
            ).reset_index(drop=True)

            # Finalize Date based on Priority
            temp_df["output"] = empty_date
            temp_df["flag"] = pd.isna(temp_df["output"])
            for col_name in df_decision.ColName:
                temp_df["flag"] = pd.isna(temp_df["output"]) | (
                    temp_df["output"] == empty_date
                )
                temp_df.loc[temp_df.flag, "output"] = temp_df.loc[
                    temp_df.flag, col_name
                ]

        else:
            temp_df.loc[:, "output"] = pd.to_datetime(
                temp_df["DateClean"], format="%m-%d-%y", errors="coerce"
            )

        out_df = temp_df["output"]
        return out_df

    def clean_meta_data(self, dataset, col_name, action, ls_date_formats=[]):
        '''
        Pre-Process or validate common meta data.

        Parameters
        ----------
        dataset : pandas data framme. Data for column to be processed.
        col_name : string. Column name.
        action : string.
            clean: pre-process column of interest
            validity: check if entry in column is valid

        ls_date_formats : list, optional
            expected date formats. The default is [].

        Returns
        -------
        pandas data series.
            series of flags: when action is validity
            series of text: when action is clean

        '''
        data = dataset.copy()
        del dataset
        # Rename columns
        data = data.rename(columns={data.columns[0]: "text_org"})
        data["text_org"] = data["text_org"].astype(str)

        if action == "clean":
            if col_name == "SerialNumber":
                data["text"] = data["text_org"].copy()
                data["output"] = data["text"].apply(
                    lambda x: re.sub("[\W_]+", "", str(x))
                )
                # Format Text
                data["output"] = data["output"].replace(np.nan, "", regex=True)
                data["output"] = data["output"].str.upper()

            elif col_name == "Date":
                # Clean Dates
                data["DateClean"] = data["text_org"].copy()
                data["DateClean"] = data["DateClean"].str.strip()
                data["DateClean"] = data["DateClean"].apply(
                    lambda x: x.replace("//", "-")
                )
                data["DateClean"] = data["DateClean"].apply(
                    lambda x: x.replace("/", "-")
                )
                data["DateClean"] = data["DateClean"].apply(
                    lambda x: x.replace(" ", "-")
                )

                # Initialize
                if len(ls_date_formats) == 0:
                    ls_date_formats = [
                        "%d-%m-%y",
                        "%d-%m-%Y",
                        "%m-%d-%y",
                        "%m-%d-%Y",
                        "%b-%y",
                        "%b-%Y",
                        "%m-%Y",
                        "%m-%y",
                    ]
                df_decision = pd.DataFrame({"DateFormat": ls_date_formats})
                df_decision["Count"] = 0
                df_decision["ColName"] = ""

                # Try all date formats
                for ix_form in range(len(ls_date_formats)):
                    col_name = f"DF_{ix_form}"
                    form = ls_date_formats[ix_form]
                    data.loc[:, col_name] = pd.to_datetime(
                        data["DateClean"], format=form, errors="coerce"
                    )

                    n_format = pd.notna(data[col_name]).sum()
                    df_decision.loc[
                        df_decision["DateFormat"] == form, "Count"
                    ] = n_format
                    df_decision.loc[
                        df_decision["DateFormat"] == form, "ColName"
                    ] = col_name

                # Prioratize Date format (based on suucess rate)
                df_decision = df_decision.sort_values(
                    by=["Count"], ascending=False
                ).reset_index(drop=True)

                # Finalize Date based on Priority
                data["output"] = np.nan
                data["flag"] = pd.isna(data["output"])
                for col_name in df_decision.ColName:
                    data.loc[data.flag, "output"] = data.loc[data.flag, col_name]
                    data["flag"] = pd.isna(data["output"])
        elif action == "validity":
            data["text"] = data["text_org"].replace(np.nan, "", regex=True)
            data["text"] = data["text"].str.upper()

            if col_name == "SerialNumber":
                ls_not_ups = [
                    "BATT",
                    "DUP",
                    "UNKNOWN",
                    "SWAP",
                    "BREAKER",
                    "DONOT",
                    "EOSL",
                    "Test",
                    "VOID",
                    "WRONG",
                    "CTO",
                    "UNKN",
                    "TEMP",
                    "SHIP",
                    "FIRSTLIGHT",
                    "XYZ",
                    "ABC",
                    "XXX",
                    "YYY",
                    "ZZZ",
                    "REPLACE",
                    "SBM",
                    "INACTIVE",
                    "NOTUSED",
                    "MOCKNUMBER",
                    "LABUNIT",
                    "BETA",
                    "OUTOFSERVICE",
                    "DAMAGEDINFREIGHT",
                ]
                TH_STR_LEN = 4
            elif col_name == "CTO":
                ls_not_ups = [
                    "CELL",
                    "WATCH",
                    "WET",
                    "CAB",
                    "BATT",
                    "DUP",
                    "BREAKER",
                    "DONOT",
                    "CHRGRS",
                    "CAT",
                    "CENT",
                ]
                TH_STR_LEN = 0
            elif col_name == "ModelId":
                ls_not_ups = [
                    "CELL",
                    "WATCH",
                    "WET",
                    "CAB",
                    "DUP",
                    "BREAKER",
                    "DONOT",
                    "9320",
                    "MBM",
                    "PDR",
                    "EBM",
                    "QFE",
                    "R1500VA",
                    "RPP",
                    "SBM",
                    "IDC",
                    "BATT",
                ]
                TH_STR_LEN = 0
            elif col_name == "ContractCoverage":
                TH_STR_LEN = 3  # (minimum length required is 4 characters)

            # Flag baed on string length
            data["flag_Valid"] = data["text"].str.len()
            data["flag_Valid"] = data["flag_Valid"] > TH_STR_LEN

            # Flag baed on string
            if "ls_not_ups" in locals():
                non_ups_regex = "|".join(ls_not_ups)
                data["flag_text"] = ~data["text"].str.contains(
                    non_ups_regex, regex=True
                )
            else:
                data["flag_text"] = True

            data["output"] = data["flag_text"] & data["flag_Valid"]

        return data["output"]

    def create_contact(self, data, dict_map, source, f_form_date,
                       ref_df_all, luk_up_tbl=pd.DataFrame()):
        """
        Create contact data for a source.

        Parameters
        ----------
        data : pandas frame. contact data.
        dict_map : dictionary for columns mapping to expected format.
        source : string. Source name e.g. InstallBase / Incident
        f_form_date : bool. Indictating if 'clean_date' function should be used
                    in case date doesnt follow any standard format

        Returns
        -------
        data : pandas dataframe. contact data in standard format.

        """
        try:
            logging.info(
                f"On line 409, starting class_ilead_contact, Size of data: {data.shape}"
            )
            # Inputs
            MINIMUM_DATE = self.TH_MIN_INSTALL_DATE

            # dict_map = dict_contact data = in_data.copy()
            ls_must_col = list(dict_map.keys())
            logging.info(
                f"On line 416 of class_ilead_contact, list of column; {ls_must_col}"
            )
            # ***** Select columns if Interest *****
            for col in ls_must_col:
                # col = ls_must_col[0]
                cur_name = dict_map[col]
                logging.info(
                    f"On line 420 in class_ilead_contact,{col}, {cur_name} ")

                # Add column if doesnt exist
                if (cur_name == "") | (cur_name not in data.columns):
                    if col != "Serial Number":
                        logging.info(f"\tAdded columns for {source} : {col}")
                        val = source if col == "Source" else ""
                        data[col] = val
                else:
                    # Rename columns if doesnt match
                    if col != cur_name:
                        data = data.rename(columns={cur_name: col})

            if source == "Contacts" and False:
                # luk_up_tbl = dict_output['InstallBase'][['Serial Number', 'Site_Number']]
                data = data.merge(luk_up_tbl, how="left",
                                  on="Site_Number", suffixes=(False, False))
                del luk_up_tbl

            data = data[ls_must_col]
        except Exception as e:
            logging.info(f"Error: {e}")
            return e

        # ***** Format Data *****
        # Serial Number
        try:
            data["SerialNumberOrg"] = data["Serial Number"].copy()
            data["Serial Number"] = self.clean_meta_data(
                data[["SerialNumberOrg"]], "SerialNumber", "clean"
            )
            data["Serial Number"] = data["Serial Number"].str.upper()

            data["DateOrg"] = data["Date"].copy()

            # Date
            if f_form_date:
                data["Date"] = self.clean_date(data[["DateOrg"]])
                data["Date"] = pd.to_datetime(
                    data["Date"], format="%Y-%m-%d", errors="coerce"
                )
            else:
                data["Date"] = pd.to_datetime(data["Date"], errors="coerce")

            # Filter Data
            ls_col = ["Serial Number", "Date"]
            for col in ls_col:
                data = data[pd.notna(data[col])]

            data["f_valid_SrNum"] = self.clean_meta_data(
                data[["Serial Number"]], "SerialNumber", "validity"
            )
            data = data[data["f_valid_SrNum"]]

            data = data.drop_duplicates(subset=["Serial Number", "Date"])
            data = data[data.Date >= MINIMUM_DATE]

            # Capitalize
            ls_col = ["Party_Name", "Site_Name", "Name", "Title", "City"]
            for col in ls_col:
                data[col] = data[col].fillna("").str.title()

            # Fill NA by blanks
            data = data.fillna("")

            # Categorize Serial Number
            data.reset_index(inplace=True, drop=True)
            ls_col_out = ["Contact_Type", "Contact_Name"]
            data.loc[:, ls_col_out] = self.classify_contact(
                data[["Serial Number", "Party_Name", "Email"]], ref_df_all
            )
            data.loc[:, "flag_EndUser"] = data["Contact_Type"] == "End Customer"

            # Group by Serial Number and choose record with max end date
            # flag the lateset date as flag_Latest based on Serial Number and Date

            max_date_df = data.groupby(by=['Serial Number']
                                       )['Date'].max().reset_index()
            max_date_df.columns = ['Serial Number', 'max_date']
            data = data.merge(max_date_df, how='left', on='Serial Number')
            data['flag_latest_date'] = data['Date'] == data['max_date']


            data = data.drop_duplicates(
                subset=["Serial Number", "Date", "flag_EndUser"]
            )

            ls_must_col_n = ls_must_col[:]
            ls_must_col_n.append('flag_EndUser')
            ls_must_col_n.append('Contact_Type')
            ls_must_col_n.append('Contact_Name')
            ls_must_col_n.append('flag_latest_date')

            data = data[ls_must_col_n]
            logging.info(f"On line 487, data: {data.shape}")
        except Exception as e:
            logging.info(f"From line 488, Error: {e}")
            return e

        return data

    def format_reference_file(self, ref_df):
        global ls_txt_eq, min_pat_th

        ls_txt_eq = ['E Technologies', 'Power Solutions', 'Data Systems',
                     'DATA CENTER SOLUTIONS', 'Insight', 'atman', 'iVision',
                     'ccorp.com', 'criticalpower.com', 'controlsys.com']
        ls_txt_eq = [(str.lower(col)) for col in ls_txt_eq]
        min_pat_th = 5
        ref_df_all = ref_df.copy()

        # Drop entries
        ref_df_all = ref_df_all[ref_df_all.Flag_keep == True]

        # Format columns in Reference File
        ls_col = ['CompanyNameAlias', 'CompanyName', 'EmailDomainName']
        ref_df_all.loc[:, ls_col] = ref_df_all[ls_col].fillna(
            '').astype(str)
        ref_df_all.loc[ref_df_all.flag_companynamealiase ==
                       False, 'CompanyNameAlias'] = ''
        ref_df_all.loc[ref_df_all.flag_emaildomain ==
                       False, 'EmailDomainName'] = ''
        ls_col_drop = ['flag_companynamealiase',
                       'flag_emaildomain', 'Flag_keep', 'CompanyName_orininal']
        ref_df_all.drop(ls_col_drop, axis=1, inplace=True)

        # Pattern Equal
        ref_df_all['pattern'] = ref_df_all[ls_col].apply(
            lambda x: ';'.join(filter(lambda ele: (len(ele) > 0),
                                      x)), axis=1)
        ref_df_all['pattern'] = ref_df_all['pattern'].str.lower()

        ref_df_all['pattern_equal'] = ref_df_all['pattern'].apply(
            lambda x:
                ';'.join(list(
                    [(y) for y in x.split(';')
                     if ((len(y) <= min_pat_th) | (y in ls_txt_eq))]
                ))
        )

        ref_df_all['pattern_txt_match'] = ref_df_all['pattern'].apply(
            lambda x:
                ';'.join(list(
                    [(y) for y in x.split(';')
                     if ((len(y) > min_pat_th) & (y not in ls_txt_eq))]
                ))
        )
        return ref_df_all

    def get_domain(self, email):
        try:
            out = email.split('@')[1]
        except:
            out = ''
        return out

    def func_cat(self, t_data_org, pattern_equal, pattern_txt_match):
        t_data = t_data_org.copy()
        del t_data_org

        txt_equal = np.array(pattern_equal.split(';'))
        txt_finds = '|'.join(pattern_txt_match.split(';'))

        if len(pattern_equal) == 0:
            t_data.loc[:, 'flag_all'] = t_data['comp_email'].apply(
                lambda x: (
                    (re.search(txt_finds, x) is not None)
                ))

        elif pattern_txt_match == '':
            ls_col = ['CompanyName', 'domain']
            t_data.loc[:, 'flag_all'] = t_data[ls_col].apply(
                lambda x: (
                    any([any(txt_equal == x[0]), any(txt_equal == x[1])])
                ), axis=1)
        else:
            # comp_email column is concatenation with name and domain
            ls_col = ['CompanyName', 'domain', 'comp_email']
            t_data.loc[:, 'flag_all'] = t_data[ls_col].apply(
                lambda x: (
                    any([any(txt_equal == x[0]), any(txt_equal == x[1])]) |
                    (re.search(txt_finds, x[2]) is not None)
                ), axis=1)
        return t_data['flag_all']

    def classify_contact(self, t_data, ref_df_all):
        """

        Parameters
        ----------
        data_mod : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        data_mod = t_data.copy()
        # taking serial number, company name and email id
        data_mod.columns = ["Serial Number", "CompanyName", "Email"]
        del t_data

        # ***** Code Constants *****
        if True:
            global ls_txt_eq, min_pat_th
            # regex_email = r"\"?([-a-zA-Z0-9.`?{}]+@\w+\.\w+)\"?"
            # regex_email = re.compile(regex_email)

        # ***** Format Data *****
        if True:
            ls_col = ['CompanyName', 'Email']
            for col in ls_col:
                # col = ls_col[0]
                data_mod.loc[:, col] = data_mod[col].fillna('')
                data_mod.loc[:, col] = data_mod[col].astype(str).str.lower()
            del col

            data_mod['domain'] = data_mod["Email"].apply(
                lambda x: self.get_domain(x))

            data_mod.loc[:, 'comp_email'] = (data_mod["CompanyName"] + ":"
                                             + data_mod["domain"])

            data_unique = data_mod[['comp_email',
                                    'CompanyName', 'domain']].copy()
            data_unique = data_unique.drop_duplicates().reset_index(drop=True)

        # ***** Classify Contact *****
        out_all = pd.DataFrame()
        for row_ix in ref_df_all.index:
            # row_ix = 0

            # Category Details
            ac_info = ref_df_all.loc[row_ix, :]
            cur_cat = ac_info.Category
            cur_comp = ac_info.CompanyName

            ls_col = ['CompanyName', 'domain', 'comp_email']
            data_unique.loc[:, 'flag_cur'] = self.func_cat(data_unique[ls_col],
                                                           ac_info.pattern_equal,
                                                           ac_info.pattern_txt_match)

            # Update Output Table for Contact
            # if any matching occurs:-
            if any(data_unique['flag_cur']):
                ls_flags = ['flag_cur']
                temp_df = data_unique.loc[data_unique['flag_cur'], :].copy()

                # Drop unnecessary flags
                temp_df.drop(ls_flags, axis=1, inplace=True)

                # Update Contact Type
                temp_df['Contact_Type'] = cur_cat
                temp_df['Contact_Name'] = cur_comp

                if out_all.shape[0] == 0:
                    out_all = temp_df.copy()
                else:
                    out_all = pd.concat([out_all, temp_df])
                del temp_df

                data_unique = data_unique.loc[
                    (data_unique['flag_cur'] == False), :].copy()
                data_unique.drop(ls_flags, axis=1, inplace=True)

        # Update Contact Type
        if data_unique.shape[0] > 0:
            data_unique['Contact_Type'] = "End Customer"
            data_unique['Contact_Name'] = "-"
            out_all = pd.concat([out_all, data_unique])

        # Format Data
        out_all = out_all.fillna('')
        out_all['CompanyName'] = out_all.CompanyName.str.title()
        out_all['Contact_Name'] = out_all.Contact_Name.str.title()

        # Foramt output
        ls_cols = ['comp_email', 'Contact_Type', 'Contact_Name']
        data_mod_1 = data_mod.copy()
        data_mod_1 = data_mod_1.merge(out_all[ls_cols],
                                      on='comp_email', how='left')

        # return out_all, data_mod_1
        return data_mod_1[["Contact_Type", "Contact_Name"]]


# %%
