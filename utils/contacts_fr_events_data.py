
"""@file
This file contains logic for contact information(name, address, contact,
number, email) extraction from a given string

@brief
@details

@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% Importing the required libraries
import re
import os
import string
import usaddress
from utils import IO
#path = os.getcwd()
#path = os.path.join(path.split('ileads_lead_generation')[0],
                    'ileads_lead_generation')
#os.chdir(path)

class DataExtraction:
    """
    This class provides method for contact information extraction from text
    columns.
    """

    def __init__(self):
        self.config = IO.read_json(mode="local", config={
            "file_dir": "./references/", "file_name": "config_dcpd.json"})

    def extract_contact_no(self, txt):
        """
        Method to extract contact number from input string
        @param txt: Input string
        @return: Comma seperated contact_number list
        """
        if type(txt) != str:
            raise TypeError(
                "DataExtraction class extract_contact_name method argument txt"
                " is not a string"
            )
        if len(txt) == 0:
            return ""

        pat_contact_no = self.config["contact_extraction"]["pat_contact_no"]
        res = re.findall(pat_contact_no, txt)
        res = [item for tpl in res for item in tpl if len(item) > 0]

        return ", ".join(list(res))


    def extract_email(self, txt):
        """
        Method to extract email_id from input string
        @param txt: Input string
        @return: Comma seperated email_id list
        """
        if type(txt) != str:
            raise TypeError(
                "DataExtraction class extract_contact_name method argument txt"
                " is not a string"
            )
        if len(txt) == 0:
            return ""

        pat_email = self.config["contact_extraction"]["pat_email"]
        txt = str.split(str.replace(txt, "\n", " "), " ")
        res = [(val) for val in txt if re.search(pat_email, val)]

        return ", ".join(list(res))


    def extract_address(self, txt, pat_address):
        """
        Method to extract address from input string
        @param txt: Input string
        @return: Address String
        """
        if type(txt) != str:
            raise TypeError(
                "DataExtraction class extract_contact_name method argument txt"
                " is not a string"
            )
        if len(txt) == 0:
            return ""

        address = ""
        txt_parts = str.split(txt, "\r\n\r\n")

        if len(txt_parts) == 1:
            txt_parts = str.split(txt, "\n\n")

        if len(txt_parts) == 1:
            txt_parts = str.split(txt, "\n")

        for txt_part in txt_parts:
            txt_part = txt_part.replace("\n", " ").replace( "\r", " ")

            ided_txt = usaddress.parse(txt_part)

            ls_valid = [(ids[1] in ["StateName", "AddressNumber"]) for ids in ided_txt]
            has_us_state = re.search(pat_address, str.replace(str.lower(txt_part), ",", ""))

            if any(ls_valid) | (has_us_state != None):
                address += txt_part if len(address)==0 else ("\n\n" + txt_part)

        address = address.replace("\n", " ")
        return address


    def extract_contact_name(self, txt):
        """
        Method to extract name from input string
        @param txt: Input string
        @return: Comma seperated Name list
        """
        if type(txt) != str:
            raise TypeError(
                "DataExtraction class extract_contact_name method argument txt"
                " is not a string"
            )
        if len(txt) == 0:
            return ""
        punc = string.punctuation + " -" + "0123456789" + "\r\n"

        non_contact = self.config["contact_extraction"]["non_contact"]
        txt_parts = str.split(txt, "\r\n\r\n")
        ls_pat_contact_name = ["contact there",  "contact", "poc"] # "contact"
        pat_contact_no = self.config["contact_extraction"]["pat_contact_no"]
        out = []

        flag_contact_there = False
        for pat_contact_name in ls_pat_contact_name:
        # pat_contact_name = ls_pat_contact_name[1]
            res = [(val) for val in txt_parts if re.search(pat_contact_name, str.lower(val))]
            if flag_contact_there and pat_contact_name == "contact":
                continue
            if len(res) > 0:
                flag_contact_there = True
                res = str.lower(res[0])

                res = res[(res.find(pat_contact_name) + len(pat_contact_name) ):]
                res = res.lstrip(punc)
                res = res.replace("mr.", "mr").replace("mrs.", "mrs").replace("ms.", "ms")
                res = re.split('[?.,@0123456789(:]', re.split('\r\n', res)[0])
                res = res[0]

                if (re.search(non_contact, str.lower(res)) is None):
                    out += [res.title()]

        if len(out) > 0:
            txt = re.sub("|".join(out), "", txt)



        txt_parts = str.split(txt, "\r\n")
        res = [val for val in txt_parts if re.search(pat_contact_no, str.lower(val))]

        if len(res) > 0:
            for text in res:
                # text = res[0]
                text = re.sub(pat_contact_no, '', text)
                if len(text) > 0:
                    text = text.lstrip(punc).rstrip(punc)
                    if (len(text) > 2) & (re.search(non_contact, str.lower(text)) is None):
                        out += [text if len(out) == 0 else ("\n" + text)]

        for i in range(len(out)):
            name = out[i]
            out[i] = name.lstrip().rstrip()
        if len(out) > 0:
            out = "\n".join(out)
        else:
            out =""

        out = out.replace("\n", ", ")
        return out

