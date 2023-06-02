
"""@file DCPD.py



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% *** Setup Environment ***

from utils import AppLogger
logger = AppLogger(__name__)

from lead_generation.base import LeadGeneration
from utils.dcpd import InstallBase
from utils.dcpd import Contract
# %% *** Define Class ***

class DCPD(LeadGeneration):

    def __init__(self):
        try:
            step_ = 'Install Base'
            status = self.etl_installbase()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Contract'
            status = self.etl_contracts()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Services'
            status = self.etl_services()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Contact'
            status = self.etl_contacts()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Lead Management'
            status = self.etl_lead_management()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Lead Generation'
            status = self.lead_generation()
            logger.app_success(f"Preprocess {step_} Data")

        except Exception as e:
            logger.app_fail(f"process install for {__name__}", e)
            raise Exception from e


    def etl_installbase(self):
        try:
            obj = InstallBase()
            self.df_data = obj.main_install()
        except Exception as e:
            logger.app_fail(f"process install for {__name__}", e)
            raise Exception from e

    def etl_services(self):

        print('Implemented etl_contracts!')

    def etl_contracts(self):
        try:
            obj = Contract()
            self.df_data = obj.main_contracts()
        except Exception as e:
            logger.app_fail(f"process contract for {__name__}", e)
            raise Exception from e

    def etl_contacts(self):
        print('Implemented etl_contacts!')

    def etl_lead_management(self):
        print('Implemented etl_lead_management !')

    def etl_lead_generation(self):
        print('Implemented etl_lead_management !')

# %%
