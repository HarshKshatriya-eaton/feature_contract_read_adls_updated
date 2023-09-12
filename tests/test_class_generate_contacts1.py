"""@file test_class_generate_contact_data.py.

@brief This file used to test code for generate contacts from contracts Data




@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
import pandas as pd
from pandas._testing import assert_frame_equal

from utils.dcpd.class_generate_contacts import Contacts

obj = Contacts()

class TestContacts:
    """
    Check for the parameters to be applicable in the final contacts generated from class_generate_contacts.py
    """

    def test_prep_data(self):
        """
        Checks prep_Data method
        """
        df_data = pd.DataFrame({
            "Name": ["a", "b"],
            "Company_a": ["ABC", "BCD"],
            "Company_b": ["DEF", "GHI"]
        })
        dict_contact = {
            "Name": "Name",
            "Company": ["Company_a", "Company_b"]
        }

        df_out, dict_updated = obj.prep_data(df_data, dict_contact)
        dict_updated_base = {'Name': 'Name', 'Company': 'nc_Company'}
        df_out_base = pd.read_csv(
            "./tests/test_data/contacts/contacts_prep_data_base.csv"
        )
        assert dict_updated == dict_updated_base
        df_out_base = df_out_base.drop("Unnamed: 0", axis=1)
        assert_frame_equal(df_out, df_out_base)

    def test_extract_data(self):
        df_data = pd.read_csv("./tests/test_data/events_data_1.csv")
        df_data = df_data[["Id", "Description"]]
        df_out = obj.extract_data("events", df_data)
        df_out_base = pd.read_csv(
            "./tests/test_data/contacts/extract_data.csv"
        )
        df_out_base = df_out_base.drop("Unnamed: 0", axis=1)

        # empty values get replaced with nan when the file is saved
        # changing it back to empty before comparison
        df_out_base["email"] = df_out_base["email"].fillna("")
        assert_frame_equal(df_out, df_out_base)

    def test_exception_src(self):
        df_data = pd.read_csv(
            "./tests/test_data/contacts/contacts_exception_src.csv"
        )
        dict_contact = {
            'Serial Number': '', 'Source': '', 'Date': 'ClosedDate',
            'Party_Number': '', 'Party_Name': '', 'Site_Number': '',
            'Site_Name': '', 'Name': 'Contact_Name1__c',
            'Company': ['Company_Name__c', 'Company__c'],
            'Title': '',
            'Email': [
                'ContactEmail', 'Contact_Email1__c',
                'Contact_Email__c', 'Email__c'
            ],
            'Company_Phone': [
                'ContactPhone', 'Contact_Phone1__c', 'Contact_Phone__c',
                'Phone__c', 'Shipping_Contact_Phone__c', 'ContactMobile'
            ],
            'Fax': '', 'Address1': ['Billing_Street__c', 'Address__c'],
            'Address2': '',
            'City': [
                'Billing_City__c', 'City__c',
                'Shipping_City1__c', 'Shipping_City__c'
            ],
            'Zipcode': [
                'Billing_Zip_Code_Postal_Code__c',
                'Shipping_Zip_Code1__c', 'Shipping_Zip_Code_Postal_Code__c',
                'Zipcode__c'
            ],
            'State': [
                'Billing_State__c', 'Shipping_State1__c',
                'Shipping_State__c', 'State__c'
            ],
            'Country': [
                'Billing_Country__c', 'Country__c',
                'Shipping_Country1__c', 'Shipping_Country__c'
            ]
        }
        df_out, dict_updated = obj.exception_src(
            "services", df_data, dict_contact)
        df_out_base = pd.read_csv(
            "./tests/test_data/contacts/contacts_exception_op.csv"
        )
        dict_updated_base = {
            'Serial Number': 'SerialNumber', 'Source': '',
            'Date': 'ClosedDate', 'Party_Number': '', 'Party_Name': '',
            'Site_Number': '', 'Site_Name': '', 'Name': 'Contact_Name1__c',
            'Company': ['Company_Name__c', 'Company__c'], 'Title': '',
            'Email': [
                'ContactEmail', 'Contact_Email1__c',
                'Contact_Email__c', 'Email__c'
            ],
            'Company_Phone': [
                'ContactPhone', 'Contact_Phone1__c',
                'Contact_Phone__c', 'Phone__c',
                'Shipping_Contact_Phone__c', 'ContactMobile'
            ], 'Fax': '',
            'Address1': ['Billing_Street__c', 'Address__c'],
            'Address2': '',
            'City': [
                'Billing_City__c', 'City__c',
                'Shipping_City1__c', 'Shipping_City__c'
            ], 'Zipcode': [
                'Billing_Zip_Code_Postal_Code__c',
                'Shipping_Zip_Code1__c',
                'Shipping_Zip_Code_Postal_Code__c',
                'Zipcode__c'
            ], 'State': [
                'Billing_State__c',
                'Shipping_State1__c',
                'Shipping_State__c', 'State__c'
            ], 'Country': [
                'Billing_Country__c', 'Country__c', 'Shipping_Country1__c',
                'Shipping_Country__c'
            ]
        }

        assert dict_updated == dict_updated_base
        df_out_base = df_out_base.drop("Unnamed: 0", axis=1)

        # Handling nan in both dataframes to handle any interpretation
        # descrepency
        df_out_base = df_out_base.fillna("")
        df_out = df_out.fillna("")
        assert_frame_equal(df_out, df_out_base)

    def test_post_process(self):
        df_data = pd.read_csv(
            "./tests/test_data/contacts/df_post_process.csv"
        )
        df_out = obj.post_process(df_data)
        df_out_base = pd.read_csv("./tests/test_data/contacts/df_post_process_op.csv")

        # Resetting index because index gets updated when latest gets filtered
        df_out_base = df_out_base.drop("Unnamed: 0", axis=1)
        df_out.reset_index(inplace=True)
        df_out_base.reset_index(inplace=True)
        df_out = df_out.drop("index", axis=1)
        df_out_base = df_out_base.drop("index", axis=1)
        assert_frame_equal(df_out, df_out_base)

    def test_filter_latest(self):
        df_data = pd.read_csv(
            "./tests/test_data/contacts/df_filter_latest.csv"
        )
        df_out = obj.filter_latest(df_data)
        df_out_base = pd.read_csv(
            "./tests/test_data/contacts/df_filter_latest_op.csv"
        )

        # Resetting index because index gets updated when latest gets filtered
        df_out_base = df_out_base.drop("Unnamed: 0.1", axis=1)
        df_out.reset_index(inplace=True)
        df_out_base.reset_index(inplace=True)
        df_out = df_out.drop("index", axis=1)
        df_out_base = df_out_base.drop("index", axis=1)
        assert_frame_equal(df_out, df_out_base)


if __name__ == "__main__":
    obj_contact = TestContacts()
