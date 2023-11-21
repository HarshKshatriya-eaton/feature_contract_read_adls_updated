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

    df = IO.read_csv(
                mode,
                {'file_dir': config['file']['dir_data'],
                 'file_name': config['file']['Raw']['M2M']['file_name'],
                 'adls_config': config['file']['Raw']['adls_credentials'],
                 'adls_dir': config['file']['Raw']['M2M']
                 }
                 )
    logging.info(f"Shape of df returned:{df.shape[0]}")

    logging.info('initiating write')

    result = IO.write_csv(
            mode,
            {
                'file_dir': config['file']['dir_results'] + config['file']['dir_intermediate'],
                'file_name': config['file']['Processed']['processed_install']['file_name'],
                'adls_config': config['file']['Processed']['adls_credentials'],
                'adls_dir': config['file']['Processed']['processed_install']

            }, df)
    logging.info(f'result: {result}')

    logging.info('completed write')
    if df.shape[0]>0:
        return func.HttpResponse("This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse("Function Failed", status_code=200)
