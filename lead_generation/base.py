'''
*********************************************************************************************
* @file base.py lead generation base class
* @brief Defines the abstrct class for leads generation
* @details
             - Defines abstract methods for lead generation
             - Generate leads
*
*
* @copyright 2023 Eaton Corporation. All Rights Reserved.
* @note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used
without direct written permission from Eaton Corporation.
**********************************************************************************************
'''

from abc import ABC, abstractmethod

class LeadGeneration(ABC):

    @abstractmethod
    def lead_pipeline(self):
        pass

    @abstractmethod
    def etl_installbase(self):
        pass

    @abstractmethod
    def etl_services(self):
        pass

    @abstractmethod
    def etl_contracts(self):
        pass

    @abstractmethod
    def etl_contacts(self):
        pass

    @abstractmethod
    def etl_lead_management(self):
        pass

    @abstractmethod
    def lead_generation(self):
        pass
