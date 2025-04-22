import pandas as pd
import requests
import json
from HelperFunctions.EnvironmentVariableLoader import get_filemaker_auth_token_from_env_reg, get_filemaker_auth_token_from_env_erp

filemaker_server_fms01_url = "https://fms01.deca.be"
ERP_database_name = "DPG_ERP"
database_file_open_orders = "ML_Open_Orders"
Registratie_database_name = "DPG_registratie"
database_file_machine_names = "ML_Machine_Naam_ID"

def load_machine_names_df(logger):
    try:
        with logger.context("Machine Names"):
            authorization_token = get_filemaker_auth_token_from_env_reg()
            url = f"{filemaker_server_fms01_url}/fmi/data/vLatest/databases/{Registratie_database_name}/layouts/{database_file_machine_names}/records"
            headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {authorization_token}'}

            response = requests.get(url, headers=headers)
            response.raise_for_status()
            response_json = response.json()

            data_section = response_json.get('response', {}).get('data', [])
            
            records_data = []
            for record in data_section:
                field_data = record.get('fieldData', {})
                records_data.append(field_data)
            logger.addListRecords("Retrieved data from FileMaker", records_data)
            df = pd.DataFrame(records_data)
            logger.addDataFrameRecords("Loaded data into Dataframe", df)
            return df

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP Error: {http_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request Error: {req_err}")
        return None
    except json.JSONDecodeError as json_err:
        logger.error(f"JSON Decode Error: Could not parse response as JSON: {json_err}")
        logger.error(f"Response text: {response.text}")
        return None
    

def get_all_orders_to_plan_from_filemaker_to_df():
    authorization_token = get_filemaker_auth_token_from_env_erp()
    url = f"{filemaker_server_fms01_url}/fmi/data/vLatest/databases/{ERP_database_name}/layouts/{database_file_open_orders}/_find"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {authorization_token}'}

    find_request_data = {
        "query": [
            {
                "_k2_bu_id":  "BU002",
                "purd_purl_ART::_k2_subserieID": ">0", #een van de subserieid's was "AW: Fwd: [EXTERN] ES-tray 227x100 height 90 and 100" want dit was een of ander giga negatief nummer.... dus groter dan 0
                "flag_book": "0",
                "qty_dnr": ">0", 
                "purchase_deca::date_delivery_dnr": "*",
                "_k2_supplier_id": "8740"
            }
        ],
        "limit": 1000
    }
    data_json = json.dumps(find_request_data)

    try:
        response = requests.post(url, headers=headers, data=data_json)
        response.raise_for_status()
        response_json = response.json()

        data_section = response_json.get('response', {}).get('data', [])
        records_data = []
        for record in data_section:
            field_data = record.get('fieldData', {})
            transformed_record = {
                'subserieID': field_data.get('purd_purl_ART::_k2_subserieID'),
                'iml': str(field_data.get('purd_purl_ART::flag_iml')),
                'amount': field_data.get('qty_dnr'),
                'date_order': pd.to_datetime(field_data.get('purchase_deca::date_delivery_dnr'), format='%m/%d/%Y').strftime('%d/%m/%Y'),
                #default naar 0 want dit is niet geïmplementeerd in filemaker of mongo nu
                'priority_code': 0,
                'articleID': field_data.get('purd_purl_ART::_k1_article_id'),
                'article_description': field_data.get('purd_purl_ART::description_nl_dnr')
            }
            records_data.append(transformed_record)
            #add_retrieved_open_order_log(transformed_record)

        df = pd.DataFrame(records_data)

        #add_pre_calculation_step_log("Retrieved all open orders from FileMaker ERP database and loaded into Dataframe")
        return df

    except requests.exceptions.HTTPError as http_err:
        #add_error_log(f"HTTP Error: {http_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        #add_error_log(f"Request Error: {req_err}")
        return None
    except json.JSONDecodeError as json_err:
        #add_error_log(f"JSON Decode Error: Could not parse response as JSON: {json_err}")
        #add_error_log(f"Response text: {response.text}")
        return None
    
    