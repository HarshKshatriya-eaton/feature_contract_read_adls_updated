import datetime
import logging
import json
import os
import azure.functions as func
from utils.dcpd.class_contracts_data import Contract


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due!")
    else:
        logging.info("Python timer trigger function ran at %s", utc_timestamp)
        config_dir = os.path.join(os.path.dirname(__file__), "../config")
        config_file = os.path.join(config_dir, "config_dcpd.json")
        try:
            # Read the configuration file
            with open(config_file, "r") as config_file:
                config = json.load(config_file)
            logging.info(f"config file: {config}")
            conf_env = config.get("conf.env", "azure-adls")
            # Create an instance of InstallBase and call main_install
            logging.info(f"mode:{conf_env}\n,config: {config}")
            obj = Contract(conf_env, config)
            logging.info("before calling main_install")
            obj.main_contracts()

            return "Success"
        except Exception as e:
            return str(e)
