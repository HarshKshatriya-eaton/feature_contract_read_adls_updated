"""@file test_events_description_extraction.py.

@brief This file used to test code for Extraction of information from events
data

@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
import pandas as pd

from utils.contacts_fr_events_data_final_v2 import DataExtraction
from utils import IO

data_extractor = DataExtraction()
config = IO.read_json(mode='local', config={
            "file_dir": '../references/', "file_name": 'config_dcpd.json'})

class TestExtract:
    """
    Checks for the contacts extracted from the events data
    """
    def test_extract_name(self):
        """
        Checks the extraction of names
        """
        df_data = pd.read_csv("test_data/events/events_data_1.csv")
        df_data.loc[:, "contact_name"] = df_data.Description.apply(
            lambda x: data_extractor.extract_contact_name(x))

        contact_name = df_data["contact_name"].astype(str)[0]
        assert contact_name == "Vincent Pelliccio"

    def test_extract_contact(self):
        """
        Checks the extraction of contact number
        """
        df_data = pd.read_csv("test_data/events/events_data_1.csv")
        df_data.loc[:, "contact"] = df_data.Description.apply(
            lambda x: data_extractor.extract_contact_no(x))

        contact = df_data["contact"].astype(str)[0]
        assert contact == "276 356 8838"

    def test_extract_email(self):
        """
        Checks the extraction of email
        """
        df_data = pd.read_csv("test_data/events/events_data_2.csv")
        df_data.loc[:, "email"] = df_data.Description.apply(
            lambda x: data_extractor.extract_email(x))

        email = df_data["email"].astype(str)[0]
        assert email == "anthony.white@coresite.com"

    def test_extract_address(self):
        """
        Checks the extraction of address
        """
        usa_states = config['output_contacts_lead']["usa_states"]
        pat_state_short = ' ' + ' | '.join(list(usa_states.keys())) + ' '
        pat_state_long = ' ' + ' | '.join(list(usa_states.values())) + ' '
        pat_address = str.lower(
            '(' + pat_state_short + '|' + pat_state_long + ')')

        df_data = pd.read_csv("test_data/events/events_data_1.csv")
        df_data.loc[:, "address"] = df_data.Description.apply(
            lambda x: data_extractor.extract_address(x, pat_address))

        address = df_data["address"].astype(str)[0]
        assert address == "700 College Road East,  Princeton " \
                                     "NJ 08540.  St, City: US, NJ, Princeton" \
                                     "  Building: PRINCETON FORRESTAL  " \
                                     "Floor: FLOOR 01  Area: MECHANICAL  " \
                                     "Location within Area: Princeton  " \
                                     "Address: PRINCETON FORRESTAL,  " \
                                     "PRINCETON, NJ, 08540-6689, US"


if __name__ == "__main__":
    obj_contact = TestExtract()
