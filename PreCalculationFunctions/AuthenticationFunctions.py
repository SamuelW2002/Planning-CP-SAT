import requests
import base64
import json
import os
from HelperFunctions.EnvironmentVariableLoader import get_filemaker_username_from_env, get_filemaker_password_from_env, get_filemaker_auth_token_from_env_reg, get_filemaker_auth_token_from_env_erp

filemaker_server_fms01_url = "https://fms01.deca.be"
Registratie_database_name = "DPG_registratie"

ERP_database_name = "DPG_ERP"
database_file_open_orders = "ML_Open_Orders"


def logout_filemaker_data_api():
    authorization_token_reg = get_filemaker_auth_token_from_env_reg()
    authorization_token_erp = get_filemaker_auth_token_from_env_erp()
    url_reg = f"{filemaker_server_fms01_url}/fmi/data/vLatest/databases/{Registratie_database_name}/sessions/{authorization_token_reg}"
    url_erp = f"{filemaker_server_fms01_url}/fmi/data/vLatest/databases/{Registratie_database_name}/sessions/{authorization_token_erp}"

    try:
        requests.delete(url_reg)
        #add_pre_calculation_step_log(f"Logged out of filemaker with url {url_reg}")
        requests.delete(url_erp)
        #add_pre_calculation_step_log(f"Logged out of filemaker with url {url_erp}")
    except requests.exceptions.HTTPError as http_err:
        return
        #add_error_log(f"HTTP Error: {http_err}")

def authorize_filemaker_data_api(logger):
    with logger.context("Authorizing Filemaker API"):
        url_erp = f"{filemaker_server_fms01_url}/fmi/data/vLatest/databases/{ERP_database_name}/sessions"
        logger.info(f"Set URL for ERP database to: {url_erp}")
        url_reg = f"{filemaker_server_fms01_url}/fmi/data/vLatest/databases/{Registratie_database_name}/sessions"
        logger.info(f"Set URL for Registry database to: {url_reg}")

        username = get_filemaker_username_from_env()
        password = get_filemaker_password_from_env()
        logger.info("Retrieved username and password from environment variables")

        base64String = base64_encode_credentials(username, password)
        headers = {'Content-Type': 'application/json', 'Authorization': base64String}

        response = requests.post(url_erp, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        token = response_json.get('response', {}).get('token')

        if token:
            os.environ['FILEMAKER_AUTH_TOKEN_ERP'] = token
            logger.info("Logged into ERP database and stored authentication token inside environment variable")
        else:
            logger.error(f"Error: Could not retrieve authentication token for ERP database, response: {response_json}")

        response = requests.post(url_reg, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        token = response_json.get('response', {}).get('token')

        if token:
            os.environ['FILEMAKER_AUTH_TOKEN_REG'] = token
            logger.info("Logged into Registry database and stored authentication token inside environment variable")
        else:
            logger.error(f"Error: Could not retrieve authentication token for Registry database, response: {response_json}")
    
    
def base64_encode_credentials(username, password):
    credentials_string = f"{username}:{password}"
    credentials_bytes = credentials_string.encode('utf-8')
    base64_bytes = base64.b64encode(credentials_bytes)
    base64_string = base64_bytes.decode('utf-8')
    return "Basic " + base64_string