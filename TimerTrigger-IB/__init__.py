import datetime
import logging
import azure.functions as func
import json
import os
from utils.dcpd.class_installbase import InstallBase

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    config_dir = "ileads_lead_generation\references"  
    config_file = os.path.join(config_dir, "config_dcpd.json") 
    try:
        # Read the configuration file
        with open(config_file, 'r') as config_file:
            config = json.load(config_file)

        conf_env = config.get("conf.env", "azure")
        # Create an instance of InstallBase and call main_install
        obj = InstallBase(self,conf_env,config)
        result = obj.main_install()


    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
