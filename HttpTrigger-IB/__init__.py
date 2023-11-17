import logging
import json
import os
import azure.functions as func
from utils.dcpd.class_installbase import InstallBase


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
  
    config_dir = os.path.join(os.path.dirname(__file__), "../config")
    config_file = os.path.join(config_dir, "config_dcpd.json") 
    try:
        # Read the configuration file
        with open(config_file, 'r') as config_file:
            config = json.load(config_file)

        conf_env = config.get("conf.env", "azure-adls")
        # Create an instance of InstallBase and call main_install
        obj = InstallBase(conf_env,config)
        result = obj.main_install()

        return func.HttpResponse(f"Function result: {result}", mimetype="text/plain")
    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=600)
