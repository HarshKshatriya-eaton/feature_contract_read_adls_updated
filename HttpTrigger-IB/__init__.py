import logging
import json
import os
import azure.functions as func
from utils.dcpd.class_installbase import InstallBase

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    mode = req.params.get('mode')

    if not mode:
        return func.HttpResponse("Please provide a 'mode' query parameter.", status_code=400)


    config_dir = "ileads_lead_generation\references"  

    if mode == 'local':
        config_file = os.path.join(config_dir, "config_dcpd.json")
    elif mode == 'azure':
        config_file = os.path.join(config_dir, "azure_config_dcpd.json")
    else:
        return func.HttpResponse("Unsupported mode. Please specify 'local' or 'azure' for the 'mode' query parameter.", status_code=400)

    try:
        # Read the configuration file
        with open(config_file, 'r') as config_file:
            config = json.load(config_file)

        # Create an instance of InstallBase and call main_install
        obj = InstallBase(mode, config)
        result = obj.main_install()

        return func.HttpResponse(f"Function result: {result}", mimetype="text/plain")
    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
