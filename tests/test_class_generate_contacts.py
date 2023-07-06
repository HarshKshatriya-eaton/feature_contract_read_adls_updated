"""@file test_class_generate_contact_data.py.

@brief This file used to test code for generate contacts from contracts Data




@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

import pytest
import pandas as pd
from datetime import datetime
from pandas._testing import assert_frame_equal

from utils.dcpd.class_generate_contacts import Contacts

obj_contact = Contacts()

class TestContacts:
    """
    Check for the parameters to be applicable in the final contacts generated from class_generate_contacts.py
    """
    def test_read_file(self):
        with pytest.raises(Exception) as info:
            obj_contact.generate_contacts()
            assert info.type ==Exception


    def test_check_occurence_srnum(self):
        """
        check if all sources for a given serial number is captured in results
        """
        ref_data = pd.DataFrame([{'Id': '', 'AccountId': '', 'OwnerExpirationNotice': '', 'StartDate': '',
                                  'EndDate': '', 'BillingStreet': '', 'BillingCity': '', 'BillingState': '',
                                  'BillingPostalCode': '', 'BillingCountry': 'US', 'BillingLatitude': '',
                                  'BillingLongitude': '', 'BillingGeocodeAccuracy': '', 'BillingAddress': '',
                                  'ShippingStreet': '', 'ShippingCity': '', 'ShippingState': '',
                                  'ShippingPostalCode': '', 'ShippingCountry': '', 'ShippingLatitude': '',
                                  'ShippingLongitude': '', 'ShippingGeocodeAccuracy': '', 'ShippingAddress': '',
                                  'ContractTerm': '', 'OwnerId': '', 'Status': '', 'CompanySignedId': '',
                                  'CompanySignedDate': '', 'CustomerSignedId': '', 'CustomerSignedTitle': '',
                                  'CustomerSignedDate': '', 'SpecialTerms': '', 'ActivatedById': '',
                                  'ActivatedDate': '', 'StatusCode': '', 'Description': '', 'RecordTypeId': '',
                                  'IsDeleted': '', 'ContractNumber': '5548', 'LastApprovedDate': '', 'CreatedDate': '',
                                  'CreatedById': '', 'LastModifiedDate': '11/17/2022 21:54', 'LastModifiedById': '',
                                  'SystemModstamp': '', 'LastActivityDate': '', 'LastViewedDate': '',
                                  'LastReferencedDate': '', 'Address__c': '', 'Automatic_Roll__c': '', 'City__c': '',
                                  'Commissioning_Revenue__c': '', 'Contact_Name__c': '', 'Contract_Comments__c': '',
                                  'Contract_Stage__c': 'Closed', 'Contract_Type__c': '',
                                  'Conversion_Sold_w_Order__c': '', 'Country__c': 'US',
                                  'Customer_Tentative_Start_Up_Date__c': '', 'Customer__c': '', 'Email__c': '',
                                  'Misc_Date__c': '', 'Misc_Revenue__c': '', 'Mobile_Phone__c': '', 'Mobile__c': '',
                                  'Opportunity__c': '', 'Original_Sales_Order__c': '', 'PDI_Product_Family__c': 'PDU',
                                  'PDI_Rep__c': 'NGH Power Systems', 'PM_Contact__c': '', 'PM_Email__c': '',
                                  'PM_Mobile__c': '', 'PM_Phone__c': '', 'Payment_Frequency__c': '', 'Phone__c': '',
                                  'Product_1_Serial__c': '110-3050-1-6', 'Product_2_Serial__c': '411-1023',
                                  'Product_3_Serial__c': '', 'Project_Name__c': 'PIMA County 911 Facility',
                                  'Project_Type__c': '', 'Qty_1__c': '', 'Qty_2__c': '', 'Qty_3__c': '',
                                  'Qty_Total_del__c': '', 'Regional_Sales_Manager__c': 'John Garrett',
                                  'Renewal_Contact__c': 'Dan Anderson', 'Renewal_Email__c': '', 'Renewal_Mobile__c': '',
                                  'Renewal_Phone__c': '(602) 908-7948', 'Scheduled_Commissioning_Date__c': '',
                                  'Scheduled_Start_Up_Date__c': 'Monday, July 2, 2012',
                                  'Scheduled_Training_Date__c': '', 'Service_Coordinator_SPDI__c': '00546000000toGQAAY',
                                  'Service_Hours__c': '', 'Service_Plan__c': '',
                                  'Service_Sales_Manager_PDI__c': '00546000000toGZAAY',
                                  'Start_Up_Completed_Date__c': 'Monday, July 2, 2012', 'Start_Up_Revenue__c': '',
                                  'State__c': 'AZ', 'Time_Date__c': '', 'Time_Revenue__c': '',
                                  'Todays_Date__c': 'Monday, February 13, 2023', 'Training_Revenue__c': '',
                                  'Warranty_Expiration_Date__c': '', 'Warranty_Start_Date__c': 'Monday, July 2, 2012',
                                  'Warranty_Verification_Packet_3mos__c': '', 'Warranty_Verification_Packet__c': '',
                                  'Zipcode__c': '', 'Smiths_Contract_Number__c': '',
                                  'Smiths_SF_Id__c': '80020000003bV6IAAU', 'Qty__c': '', 'Type__c': '',
                                  'Start_date__c': '', 'Duration_of_Contract__c': '', 'Territory__c': '',
                                  'Customer_Specific__c': '', 'Bank_information__c': '', 'Bank_Name__c': '',
                                  'Sort_Code__c': '', 'Account_Name__c': '', 'Account_Number__c': '',
                                  'IBAN_Sort_Code__c': '', 'Country_c__c': '', 'Product_lines__c': '',
                                  'Name_of_Company__c': 'NGH Power', 'Website_URL_Address__c': '',
                                  'Phone_Number__c': '', 'Contract_Term__c': '', 'Market__c': '', 'Reseller__c': ''}])

        sr_num_data = pd.DataFrame([{'SerialNumber': '120-0159-3', 'ContractNumber': '5548'}])

        result = obj_contact.generate_contacts(ref_data, sr_num_data)
        count_sr_num = result['Serial_Number'].value_counts()['120-0159-3']
        expected_output = 3
        assert count_sr_num == expected_output

    def test_no_duplicate_srnum_and_source(self):
        """
        check if multiple serial nos. for the same source not present
        """
        ref_data = pd.DataFrame([{'Id': '', 'AccountId': '', 'OwnerExpirationNotice': '', 'StartDate': '',
                                  'EndDate': '', 'BillingStreet': '', 'BillingCity': '', 'BillingState': '',
                                  'BillingPostalCode': '', 'BillingCountry': 'US', 'BillingLatitude': '',
                                  'BillingLongitude': '', 'BillingGeocodeAccuracy': '', 'BillingAddress': '',
                                  'ShippingStreet': '', 'ShippingCity': '', 'ShippingState': '',
                                  'ShippingPostalCode': '', 'ShippingCountry': '', 'ShippingLatitude': '',
                                  'ShippingLongitude': '', 'ShippingGeocodeAccuracy': '', 'ShippingAddress': '',
                                  'ContractTerm': '', 'OwnerId': '', 'Status': '', 'CompanySignedId': '',
                                  'CompanySignedDate': '', 'CustomerSignedId': '', 'CustomerSignedTitle': '',
                                  'CustomerSignedDate': '', 'SpecialTerms': '', 'ActivatedById': '',
                                  'ActivatedDate': '', 'StatusCode': '', 'Description': '', 'RecordTypeId': '',
                                  'IsDeleted': '', 'ContractNumber': '5639', 'LastApprovedDate': '', 'CreatedDate': '',
                                  'CreatedById': '', 'LastModifiedDate': '3/17/2021 16:09', 'LastModifiedById': '',
                                  'SystemModstamp': '', 'LastActivityDate': '', 'LastViewedDate': '',
                                  'LastReferencedDate': '', 'Address__c': '', 'Automatic_Roll__c': '', 'City__c': '',
                                  'Commissioning_Revenue__c': '', 'Contact_Name__c': '', 'Contract_Comments__c': '',
                                  'Contract_Stage__c': 'Closed', 'Contract_Type__c': '',
                                  'Conversion_Sold_w_Order__c': '', 'Country__c': 'US',
                                  'Customer_Tentative_Start_Up_Date__c': '', 'Customer__c': '', 'Email__c': '',
                                  'Misc_Date__c': '', 'Misc_Revenue__c': '', 'Mobile_Phone__c': '', 'Mobile__c': '',
                                  'Opportunity__c': '', 'Original_Sales_Order__c': '', 'PDI_Product_Family__c': 'PDU',
                                  'PDI_Rep__c': 'NGH Power Systems', 'PM_Contact__c': '', 'PM_Email__c': '',
                                  'PM_Mobile__c': '', 'PM_Phone__c': '', 'Payment_Frequency__c': '', 'Phone__c': '',
                                  'Product_1_Serial__c': '110-3050-1-6', 'Product_2_Serial__c': '411-1023',
                                  'Product_3_Serial__c': '', 'Project_Name__c': 'PIMA County 911 Facility',
                                  'Project_Type__c': '', 'Qty_1__c': '', 'Qty_2__c': '', 'Qty_3__c': '',
                                  'Qty_Total_del__c': '', 'Regional_Sales_Manager__c': 'John Garrett',
                                  'Renewal_Contact__c': 'Dan Anderson', 'Renewal_Email__c': '', 'Renewal_Mobile__c': '',
                                  'Renewal_Phone__c': '', 'Scheduled_Commissioning_Date__c': '',
                                  'Scheduled_Start_Up_Date__c': 'Monday, July 2, 2012',
                                  'Scheduled_Training_Date__c': '', 'Service_Coordinator_SPDI__c': '00546000000toGQAAY',
                                  'Service_Hours__c': '', 'Service_Plan__c': '',
                                  'Service_Sales_Manager_PDI__c': '00546000000toGZAAY',
                                  'Start_Up_Completed_Date__c': 'Monday, July 2, 2012', 'Start_Up_Revenue__c': '',
                                  'State__c': 'AZ', 'Time_Date__c': '', 'Time_Revenue__c': '',
                                  'Todays_Date__c': 'Monday, February 13, 2023', 'Training_Revenue__c': '',
                                  'Warranty_Expiration_Date__c': '', 'Warranty_Start_Date__c': 'Monday, July 2, 2012',
                                  'Warranty_Verification_Packet_3mos__c': '', 'Warranty_Verification_Packet__c': '',
                                  'Zipcode__c': '', 'Smiths_Contract_Number__c': '',
                                  'Smiths_SF_Id__c': '80020000003bV6IAAU', 'Qty__c': '', 'Type__c': '',
                                  'Start_date__c': '', 'Duration_of_Contract__c': '', 'Territory__c': '',
                                  'Customer_Specific__c': '', 'Bank_information__c': '', 'Bank_Name__c': '',
                                  'Sort_Code__c': '', 'Account_Name__c': '', 'Account_Number__c': '',
                                  'IBAN_Sort_Code__c': '', 'Country_c__c': '', 'Product_lines__c': '',
                                  'Name_of_Company__c': 'NGH Power', 'Website_URL_Address__c': '',
                                  'Phone_Number__c': '', 'Contract_Term__c': '', 'Market__c': '', 'Reseller__c': ''}])

        sr_num_data = pd.DataFrame([{'SerialNumber': '110-4211-2', 'ContractNumber': '5639'}])

        result = obj_contact.generate_contacts()
        duplicate_srnum = (result[result.duplicated(subset=['Serial_Number','Source'],keep=False)])
        num_dup_rows = len(duplicate_srnum.index)
        expected_output = 0
        assert num_dup_rows == expected_output


    def test_merge_and_all_dtsrc_contact(self):
        """
        Check if data generated correctly with sr_num and then obtained 3 datasources
        """
        ref_data = pd.DataFrame([{'Id': '', 'AccountId': '', 'OwnerExpirationNotice': '', 'StartDate': '',
                                  'EndDate': '', 'BillingStreet': '', 'BillingCity': '', 'BillingState': '',
                                  'BillingPostalCode': '', 'BillingCountry': 'US', 'BillingLatitude': '',
                                  'BillingLongitude': '', 'BillingGeocodeAccuracy': '', 'BillingAddress': '',
                                  'ShippingStreet': '', 'ShippingCity': '', 'ShippingState': '',
                                  'ShippingPostalCode': '', 'ShippingCountry': '', 'ShippingLatitude': '',
                                  'ShippingLongitude': '', 'ShippingGeocodeAccuracy': '', 'ShippingAddress': '',
                                  'ContractTerm': '', 'OwnerId': '', 'Status': '', 'CompanySignedId': '',
                                  'CompanySignedDate': '', 'CustomerSignedId': '', 'CustomerSignedTitle': '',
                                  'CustomerSignedDate': '', 'SpecialTerms': '', 'ActivatedById': '',
                                  'ActivatedDate': '', 'StatusCode': '', 'Description': '', 'RecordTypeId': '',
                                  'IsDeleted': '', 'ContractNumber': '4863', 'LastApprovedDate': '', 'CreatedDate': '',
                                  'CreatedById': '', 'LastModifiedDate': '11/17/2022 21:54', 'LastModifiedById': '',
                                  'SystemModstamp': '', 'LastActivityDate': '', 'LastViewedDate': '',
                                  'LastReferencedDate': '', 'Address__c': '', 'Automatic_Roll__c': '', 'City__c': '',
                                  'Commissioning_Revenue__c': '', 'Contact_Name__c': '', 'Contract_Comments__c': '',
                                  'Contract_Stage__c': 'Closed', 'Contract_Type__c': '',
                                  'Conversion_Sold_w_Order__c': '', 'Country__c': 'US',
                                  'Customer_Tentative_Start_Up_Date__c': '', 'Customer__c': '', 'Email__c': '',
                                  'Misc_Date__c': '', 'Misc_Revenue__c': '', 'Mobile_Phone__c': '', 'Mobile__c': '',
                                  'Opportunity__c': '', 'Original_Sales_Order__c': '', 'PDI_Product_Family__c': 'PDU',
                                  'PDI_Rep__c': 'NGH Power Systems', 'PM_Contact__c': '', 'PM_Email__c': '',
                                  'PM_Mobile__c': '', 'PM_Phone__c': '', 'Payment_Frequency__c': '', 'Phone__c': '',
                                  'Product_1_Serial__c': '110-3050-1-6', 'Product_2_Serial__c': '411-1023',
                                  'Product_3_Serial__c': '', 'Project_Name__c': 'PIMA County 911 Facility',
                                  'Project_Type__c': '', 'Qty_1__c': '', 'Qty_2__c': '', 'Qty_3__c': '',
                                  'Qty_Total_del__c': '', 'Regional_Sales_Manager__c': 'John Garrett',
                                  'Renewal_Contact__c': 'Dan Anderson', 'Renewal_Email__c': '', 'Renewal_Mobile__c': '',
                                  'Renewal_Phone__c': '(602) 908-7948', 'Scheduled_Commissioning_Date__c': '',
                                  'Scheduled_Start_Up_Date__c': 'Monday, July 2, 2012',
                                  'Scheduled_Training_Date__c': '', 'Service_Coordinator_SPDI__c': '00546000000toGQAAY',
                                  'Service_Hours__c': '', 'Service_Plan__c': '',
                                  'Service_Sales_Manager_PDI__c': '00546000000toGZAAY',
                                  'Start_Up_Completed_Date__c': 'Monday, July 2, 2012', 'Start_Up_Revenue__c': '',
                                  'State__c': 'AZ', 'Time_Date__c': '', 'Time_Revenue__c': '',
                                  'Todays_Date__c': 'Monday, February 13, 2023', 'Training_Revenue__c': '',
                                  'Warranty_Expiration_Date__c': '', 'Warranty_Start_Date__c': 'Monday, July 2, 2012',
                                  'Warranty_Verification_Packet_3mos__c': '', 'Warranty_Verification_Packet__c': '',
                                  'Zipcode__c': '', 'Smiths_Contract_Number__c': '',
                                  'Smiths_SF_Id__c': '80020000003bV6IAAU', 'Qty__c': '', 'Type__c': '',
                                  'Start_date__c': '', 'Duration_of_Contract__c': '', 'Territory__c': '',
                                  'Customer_Specific__c': '', 'Bank_information__c': '', 'Bank_Name__c': '',
                                  'Sort_Code__c': '', 'Account_Name__c': '', 'Account_Number__c': '',
                                  'IBAN_Sort_Code__c': '', 'Country_c__c': '', 'Product_lines__c': '',
                                  'Name_of_Company__c': 'NGH Power', 'Website_URL_Address__c': '',
                                  'Phone_Number__c': '', 'Contract_Term__c': '', 'Market__c': '', 'Reseller__c': ''}])

        sr_num_data = pd.DataFrame([{'SerialNumber': '411-1023', 'ContractNumber': '4863'}])

        result = obj_contact.generate_contacts(ref_data,sr_num_data)
        result = result.loc[result['Serial_Number']=='411-1023','Source']
        data_source = ','.join(result.astype(str))
        expected_output = 'Contract,PM,Renewal'

        assert data_source == expected_output

    def test_ltst_dt_same_src(self):
        """
        Check if only latest date corresponding to a given serial number from a source is captured
        """
        ref_data = pd.DataFrame([{'Id': '', 'AccountId': '', 'OwnerExpirationNotice': '', 'StartDate': '',
                                  'EndDate': '', 'BillingStreet': '', 'BillingCity': '', 'BillingState': '',
                                  'BillingPostalCode': '', 'BillingCountry': 'US', 'BillingLatitude': '',
                                  'BillingLongitude': '', 'BillingGeocodeAccuracy': '', 'BillingAddress': '',
                                  'ShippingStreet': '', 'ShippingCity': '', 'ShippingState': '',
                                  'ShippingPostalCode': '', 'ShippingCountry': '', 'ShippingLatitude': '',
                                  'ShippingLongitude': '', 'ShippingGeocodeAccuracy': '', 'ShippingAddress': '',
                                  'ContractTerm': '', 'OwnerId': '', 'Status': '', 'CompanySignedId': '',
                                  'CompanySignedDate': '', 'CustomerSignedId': '', 'CustomerSignedTitle': '',
                                  'CustomerSignedDate': '', 'SpecialTerms': '', 'ActivatedById': '',
                                  'ActivatedDate': '', 'StatusCode': '', 'Description': '', 'RecordTypeId': '',
                                  'IsDeleted': '', 'ContractNumber': '5639', 'LastApprovedDate': '', 'CreatedDate': '',
                                  'CreatedById': '', 'LastModifiedDate': '3/17/2021 16:09', 'LastModifiedById': '',
                                  'SystemModstamp': '', 'LastActivityDate': '', 'LastViewedDate': '',
                                  'LastReferencedDate': '', 'Address__c': '', 'Automatic_Roll__c': '', 'City__c': '',
                                  'Commissioning_Revenue__c': '', 'Contact_Name__c': '', 'Contract_Comments__c': '',
                                  'Contract_Stage__c': 'Closed', 'Contract_Type__c': '',
                                  'Conversion_Sold_w_Order__c': '', 'Country__c': 'US',
                                  'Customer_Tentative_Start_Up_Date__c': '', 'Customer__c': '', 'Email__c': '',
                                  'Misc_Date__c': '', 'Misc_Revenue__c': '', 'Mobile_Phone__c': '', 'Mobile__c': '',
                                  'Opportunity__c': '', 'Original_Sales_Order__c': '', 'PDI_Product_Family__c': 'PDU',
                                  'PDI_Rep__c': 'NGH Power Systems', 'PM_Contact__c': '', 'PM_Email__c': '',
                                  'PM_Mobile__c': '', 'PM_Phone__c': '', 'Payment_Frequency__c': '', 'Phone__c': '',
                                  'Product_1_Serial__c': '110-3050-1-6', 'Product_2_Serial__c': '411-1023',
                                  'Product_3_Serial__c': '', 'Project_Name__c': 'PIMA County 911 Facility',
                                  'Project_Type__c': '', 'Qty_1__c': '', 'Qty_2__c': '', 'Qty_3__c': '',
                                  'Qty_Total_del__c': '', 'Regional_Sales_Manager__c': 'John Garrett',
                                  'Renewal_Contact__c': 'Dan Anderson', 'Renewal_Email__c': '', 'Renewal_Mobile__c': '',
                                  'Renewal_Phone__c': '', 'Scheduled_Commissioning_Date__c': '',
                                  'Scheduled_Start_Up_Date__c': 'Monday, July 2, 2012',
                                  'Scheduled_Training_Date__c': '', 'Service_Coordinator_SPDI__c': '00546000000toGQAAY',
                                  'Service_Hours__c': '', 'Service_Plan__c': '',
                                  'Service_Sales_Manager_PDI__c': '00546000000toGZAAY',
                                  'Start_Up_Completed_Date__c': 'Monday, July 2, 2012', 'Start_Up_Revenue__c': '',
                                  'State__c': 'AZ', 'Time_Date__c': '', 'Time_Revenue__c': '',
                                  'Todays_Date__c': 'Monday, February 13, 2023', 'Training_Revenue__c': '',
                                  'Warranty_Expiration_Date__c': '', 'Warranty_Start_Date__c': 'Monday, July 2, 2012',
                                  'Warranty_Verification_Packet_3mos__c': '', 'Warranty_Verification_Packet__c': '',
                                  'Zipcode__c': '', 'Smiths_Contract_Number__c': '',
                                  'Smiths_SF_Id__c': '80020000003bV6IAAU', 'Qty__c': '', 'Type__c': '',
                                  'Start_date__c': '', 'Duration_of_Contract__c': '', 'Territory__c': '',
                                  'Customer_Specific__c': '', 'Bank_information__c': '', 'Bank_Name__c': '',
                                  'Sort_Code__c': '', 'Account_Name__c': '', 'Account_Number__c': '',
                                  'IBAN_Sort_Code__c': '', 'Country_c__c': '', 'Product_lines__c': '',
                                  'Name_of_Company__c': 'NGH Power', 'Website_URL_Address__c': '',
                                  'Phone_Number__c': '', 'Contract_Term__c': '', 'Market__c': '', 'Reseller__c': ''}])

        sr_num_data = pd.DataFrame([{'SerialNumber': '110-4211-2', 'ContractNumber': '5639'}])

        result = obj_contact.generate_contacts(ref_data,sr_num_data)
        result_date = result.loc[(result['Serial_Number'] == '110-4211-2') & (result['Source'] == 'Contract')]
        result_date['Date']=pd.to_datetime(result_date['Date'])
        output = [{'Serial_Number': '110-4211-2', 'Source': 'Contract', 'Date': datetime.strptime('3/17/2021','%m/%d/%Y'), 'Site_Number': '', 'Site_Name': 'NGH Power', 'Name': '', 'Company': '', 'Title': '', 'Email': '', 'Company_Phone': ';;;', 'Fax': '', 'Address1': '', 'Address2': '', 'City': '', 'State': 'AZ', 'Zipcode': '', 'Country': 'US'}]
        expected_output = pd.DataFrame(output)
        assert_frame_equal(result_date,expected_output)

#Call

if __name__ == "__main__":
    obj_contact = TestContacts()
