'''
*********************************************************************************************
* @file __main__.py main function
* @brief Main function to trigger the full pipeline
* @details
             - Execute all the steps in the analytic module
             - Steps should be called in order inside run function
*
*
* @copyright 2023 Eaton Corporation. All Rights Reserved.
* @note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used
without direct written permission from Eaton Corporation.
**********************************************************************************************
'''

from lead_generation import DCPD

import traceback
from utils._helper.logger import AppLogger

vendors = {
 'dcpd': DCPD()
}

class LeadGeneration():

    def __init__(self, business):
        self.business = business
        self.logger = AppLogger(business)

    def get_generator(self):
        step_ = 'Get connector'
        try:
            generator = vendors[self.business]
            self.logger.app_debug(step_, f"{traceback.print_exc()}")

            return generator

        except Exception as e:
            self.logger.app_fail(f'{step_}: FAILED')
            raise Exception from e

    def generate_lead(self):
        try:
            generator = self.get_generator()

            step_ = 'Install Base'
            status = generator.etl_installbase()
            self.logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Contract'
            status = generator.etl_contracts()
            self.logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Services'
            status = generator.etl_services()
            self.logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Contact'
            status = generator.etl_contacts()
            self.logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Lead Management'
            status = generator.etl_lead_management()
            self.logger.app_success(f"Preprocess {step_} Data")

            # Generate leads

        except Exception as e:
            self.logger.app_fail(step_, f"{traceback.print_exc()}")
            raise Exception from e

    def reports(self, config):
        step_ = f'read data: {config["name"]}'
        try:
            generator = self.get_connector()
            data = generator.write(config)

            self.logger.app_debug(step_, f"{traceback.print_exc()}")
            return data

        except Exception as e:
            self.logger.app_fail(step_, f"{traceback.print_exc()}")
            raise Exception from e

#%%
if __name__ == "__main__":
    clg = LeadGeneration('dcpd')
    clg.generate_lead()
