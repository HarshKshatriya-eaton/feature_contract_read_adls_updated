import logging
import json
import os
import azure.functions as func
from utils.io import IO


IO = IO()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Starting function app")
    config_dir = os.path.join(os.path.dirname(__file__), "../config")
    config_file = os.path.join(config_dir, "config_dcpd.json")

    # Read the configuration file
    with open(config_file, 'r') as config_file:
        config = json.load(config_file)
    logging.info(f"Printing Config:\n")
    logging.info(config)
    mode = config.get("conf.env", "azure-adls")

    df = IO.read_csv(mode,
            {'adls_config': config['file']['Reference']['adls_credentials'],
                'adls_dir': config['file']['Reference']['decode_sr_num']
                })
    logging.info(f"Shape of df returned:{df.shape[0]}")


    if df.shape[0]>0:
        return func.HttpResponse("This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse("Function Failed", status_code=200)
