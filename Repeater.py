import requests
import boto3
import json
import csv
import datetime
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
load_dotenv()

# Init Globals from env 
SPLUNK_SERVER = os.getenv("splunk_server")
SPLUNK_TOKEN  = os.getenv("splunk_token")
TANIUM_SERVER = os.getenv("tanium_server")
TANIUM_TOKEN  = os.getenv("tanium_token")
CONFIG_FILE = "config.txt"

# Adjust library warnings to reduce noise
requests.packages.urllib3.disable_warnings()
boto3.compat.filter_python_deprecation_warnings()
logging.basicConfig(filename='example.log', level=logging.DEBUG)

class JobConfig:
    def __init__(self, config:dict):
        """Takes a row from the csv file and makes it into an object for easy referencing between functions"""
        self.job_name               = config['Name'] 
        self.destination_type       = config['Destination Type']
        self.original_file_location = config['File Location'] 
        self.frequency              = config['Frequency']
        self.last_run               = config['Last Run']
        self.tanium_type            = config['Tanium Type']
        self.tanium_component_name  = config['Component Name']
        self.file_format            = config['File Format']
        self.bucket_name            = config['Bucket Name']
        self.flatten                = config['Flatten']
        self.overwrite              = config['Overwrite']

        if self.overwrite.lower() == "yes":
            full_file_name = config['File Location'].split('/')[-1]
            name, extension = full_file_name.split('.')

            unique_name = f"{name}-{datetime.now().strftime('%Y-%m-%d')}.{extension}"

            self.file_location = config['File Location'].replace(full_file_name, unique_name)

        else:
            self.file_location = config['File Location']
             
    def dump(self):
        """Repacks the object into a list to easily rewrite the csv file on exit"""
        return {
                "Name"              :self.job_name, 
                "Destination Type"  :self.destination_type, 
                "File Location"     :self.original_file_location, 
                "Frequency"         :self.frequency, 
                "Last Run"          :self.last_run, 
                "Tanium Type"       :self.tanium_type, 
                "Component Name"    :self.tanium_component_name, 
                "File Format"       :self.file_format, 
                "Bucket Name"       :self.bucket_name, 
                "Flatten"           :self.flatten,
                "Overwrite"         :self.overwrite
        }

# GLOBAL SETUP / HELPER FUNCTIONS
def log(level: str , message: str) -> None:
    """Adds a log file entry prefixed with a timestamp"""
    if level == "info":
        logging.info(f"{datetime.now()} {message}")
        return
    if level == "debug":
        logging.debug(f"{datetime.now()} {message}")
        return
    if level == "warning":
        logging.warning(f"{datetime.now()} {message}")
        return
    if level == "error":
        logging.error(f"{datetime.now()} {message}")

def setup_boto() -> None:
    """Does a global setup for the boto3 library to communicate with AWS S3"""

    boto3.setup_default_session(aws_access_key_id=os.getenv("aws_access_key_id"),
                                aws_secret_access_key=os.getenv("aws_secret_access_key"),
                                aws_session_token=os.getenv("aws_session_token"))
    
def flatten_get_largest_list(entry:dict) -> int:
    """Used when flattening json to find the largest list in a given object so that you will know how many rows to write"""
    largest = 0
    for value in entry.values():
        if isinstance(value, list):
            if len(value) > largest:
                largest = len(value)

    
    return largest

# GET REQUEST FUNCTIONS
def get_asset_reports() -> requests.Response: 
    """Pulls the asset reports json response from the Tanium server"""
    header = {'session': TANIUM_TOKEN}
    log("info", "gathering tanium asset from tanium server")
    try: 
        return requests.get(f'https://{TANIUM_SERVER}/plugin/products/asset/private/reports', headers=header, verify=False)
    
    except requests.exceptions.HTTPError as error:
        log("error", "A generic HTTPError was thrown when attempting to retrieve asset reports. Please make sure your server URL is correct, and that your server is online.")
        log("debug", f"Request Sent: {error.request}")
        log("debug", f"Response Text: {error.response.text}") 
        return None

    except requests.exceptions.ConnectionError as error:
        log("error", "Unable to create a connection to the Tanium Server when calling the api for asset reports.") 
        log("warning", "Connection errors are usually due to server being offline or an invalid server name being provided.")
        log("debug", f"Error thrown: {error}")
        return None
    
    except Exception as error:
        log("error", "An unexpected error occurred when attempting to retrieve asset reports. Check debug log entries.")
        log("error", f"Exception Name: {type(error).__name__}")
        log("error", "Troubleshooting will need to be done to find the source of the error and a specific error handler should be made.")
        return None

def get_saved_questions() -> requests.Response: 
    """Pulls all the saved questions from the Tanium server"""
    header = {'session': TANIUM_TOKEN}
    log("info", "gathering tanium saved questions from tanium server")
    try: 
        return requests.get(f'https://{TANIUM_SERVER}/api/v2/saved_questions', headers=header, verify=False)
    
    except requests.exceptions.HTTPError as error:
        log("error", "A generic HTTPError was thrown when attempting to retrieve saved questions. Please make sure your server URL is correct, and that your server is online.")
        log("debug", f"Request Sent: {error.request}")
        log("debug", f"Response Text: {error.response.text}") 
        return None

    except requests.exceptions.ConnectionError as error:
        log("error", "Unable to create a connection to the Tanium Server when calling the api for saved questions.") 
        log("warning", "Connection errors are usually due to server being offline or an invalid server name being provided.")
        log("debug", f"Error thrown: {error}")
        return None
    
    except Exception as error:
        log("error", "An unexpected error occurred when attempting to retrieve saved questions. Check debug log entries.")
        log("debug", f"Exception Name: {type(error).__name__}")
        log("debug", "Troubleshooting will need to be done to find the source of the error and a specific error handler should be made.")
        return None

def get_asset_views() -> requests.Response: 
    """Pulls all the saved questions from the Tanium server"""
    header = {'session': TANIUM_TOKEN}
    log("info", "gathering tanium asset views from tanium server")
    try: 
        return requests.get(f'https://{TANIUM_SERVER}/plugin/products/asset/v1/views/', headers=header, verify=False)
    
    except requests.exceptions.HTTPError as error:
        log("error", "A generic HTTPError was thrown when attempting to retrieve asset views. Please make sure your server URL is correct, and that your server is online.")
        log("debug", f"Request Sent: {error.request}")
        log("debug", f"Response Text: {error.response.text}") 
        return None

    except requests.exceptions.ConnectionError as error:
        log("error", "Unable to create a connection to the Tanium Server when calling the api for asset views.") 
        log("warning", "Connection errors are usually due to server being offline or an invalid server name being provided.")
        log("debug", f"Error thrown: {error}")
        return None
    
    except Exception as error:
        log("error", "An unexpected error occurred when attempting to retrieve asset views. Check debug log entries.")
        log("debug", f"Exception Name: {type(error).__name__}")
        log("debug", "Troubleshooting will need to be done to find the source of the error and a specific error handler should be made.")
        return None

# JSON RESPONSE FILTERING FUNCTIONS
def find_asset_report_by_name(report_name: str) -> dict:
    """Requests all Asset Reports from the Tanium Server, and returns the specified report from the results"""
    all_reports = get_asset_reports()

    if not all_reports:
        return None

    if all_reports.ok:
        try:
            all_reports = json.loads(all_reports.text)

        except json.JSONDecodeError as error:
            log("error", "A JSONDecodeError was thrown when parsing the request for the Asset Reports on the Tanium Server")
            log("debug", f"The Asset Report being searched: {report_name}. Although the error was thrown when parsing all asset reports.")
            log("debug", f"JSONDecodeError message: {error.msg}")
            log("debug", f"Error was thrown processing the following: {error.doc}")
            log("debug", f"Error started as position: {error.pos}, on line {error.lineno}, column: {error.colno}")

        for report in all_reports['data']:
            if report['reportName'] == report_name:
                return report
            
        else:
            log("error", f"no corresponding report found from the provided report name: {report_name}")
            log("debug", f"json request body: {str(all_reports)}")
            return None
            
    else:
        log("error", "unable to get asset reports")

def get_saved_question_id_by_name(question_name: str) -> int:
    """Requests all Saved Questions from the Tanium Server, and returns the specified question id from the saved question that matches the provided name"""
    all_questions = get_saved_questions()

    if not all_questions:
        return None

    if all_questions.ok:
        try:
            all_questions = json.loads(all_questions.text)

        except json.JSONDecodeError as error:
            log("error", "A JSONDecodeError was thrown when parsing the request for the Saved Questions on the Tanium Server")
            log("debug", f"The Saved Question being searched: {question_name}. Although the error was thrown when parsing all saved questions.")
            log("debug", f"JSONDecodeError message: {error.msg}")
            log("debug", f"Error was thrown processing the following: {error.doc}")
            log("debug", f"Error started as position: {error.pos}, on line {error.lineno}, column: {error.colno}")

        for question in all_questions['data']:
            if question['name'] == question_name:
                return question['id']
            
        else:
            log("error", f"no corresponding question found from the provided question name: {question_name}")
            log("debug", f"json request body: {str(all_questions)}")
            return None
            
    else:
        log("error", "unable to get saved questions")

def get_asset_view_by_name(asset_view_name: str) -> dict:
    """Requests all Asset Reports from the Tanium Server, and returns the specified report from the results"""
    all_asset_view = get_asset_views()

    if not all_asset_view:
        return None

    if all_asset_view.ok:
        try:
            all_asset_view = json.loads(all_asset_view.text)

        except json.JSONDecodeError as error:
            log("error", "A JSONDecodeError was thrown when parsing the request for the Asset View on the Tanium Server")
            log("debug", f"The Asset Report being searched: {asset_view_name}. Although the error was thrown when parsing all asset views.")
            log("debug", f"JSONDecodeError message: {error.msg}")
            log("debug", f"Error was thrown processing the following: {error.doc}")
            log("debug", f"Error started as position: {error.pos}, on line {error.lineno}, column: {error.colno}")

        for view in all_asset_view['data']:
            if view['viewName'] == asset_view_name:
                return view
            
        else:
            log("error", f"no corresponding report found from the provided asset view name: {asset_view_name}")
            log("debug", f"json request body: {str(all_asset_view)}")
            return None
            
    else:
        log("error", "unable to get asset views")

# POST REQUEST FUNCTIONS   
def query_asset_report(id: int) -> dict:
    payload = {"id": id}

    try:
        report_results = requests.post(f'https://{TANIUM_SERVER}/plugin/products/asset/private/reports/{id}/query', headers={'session': TANIUM_TOKEN}, json=payload, verify=False)

    except requests.exceptions.HTTPError as error:
        log("error", f"A generic HTTPError was thrown when attempting to query asset report by ID: {id}. Please make sure your server URL/ID is correct, and that your server is online.")
        log("debug", f"Request Sent: {error.request}")
        log("debug", f"Response Text: {error.response.text}")
        return None

    except requests.exceptions.ConnectionError as error:
        log("error", "Unable to create a connection to the Tanium Server when calling the api to query asset reports results.") 
        log("warning", "Connection errors are usually due to server being offline or an invalid server name being provided.")
        log("debug", f"Error thrown: {error}")
        return None
    
    except Exception as error:
        log("error", "An unexpected error occurred when attempting to query asset report results. Check debug log entries.")
        log("debug", f"Exception Name: {type(error).__name__}")
        log("debug", "Troubleshooting will need to be done to find the source of the error and a specific error handler should be made. ")
        return None
    
    if report_results.ok:
        log("info", "report exists")
        try:
            report_results = json.loads(report_results.text)

        except json.JSONDecodeError as error:
            log("error", "A JSONDecodeError was thrown when parsing the request for querying Asset Reports results on the Tanium Server")
            log("debug", f"The Asset Report being queried: {id} from {TANIUM_SERVER}. ")
            log("debug", f"JSONDecodeError message: {error.msg}")
            log("debug", f"Error was thrown processing the following: {error.doc}")
            log("debug", f"Error started as position: {error.pos}, on line {error.lineno}, column: {error.colno}")
            return None
        
        except Exception as error:
            log("error", "An unexpected error occurred when attempting to parse asset report results. Check debug log entries.")
            log("debug", f"Exception Name: {type(error).__name__}")
            log("debug", "Troubleshooting will need to be done to find the source of the error and a specific error handler should be made.")
            return None

        return report_results

def get_saved_question_results(id:str) -> dict:
    """Return results for a saved question from the Tanium Server"""
    params = {'most_recent_flag': 1}
    try:
        request = requests.get(f"https://{TANIUM_SERVER}/api/v2/result_data/saved_question/{str(id)}", headers = {'session': TANIUM_TOKEN}, params=params)

    except requests.exceptions.HTTPError as error:
        log("error", "A generic HTTPError was thrown when attempting to retrieve saved question results. Please make sure your server URL is correct, and that your server is online.")
        log("debug", f"Request Sent: {error.request}")
        log("debug", f"Response Text: {error.response.text}")
        return None

    except requests.exceptions.ConnectionError as error:
        log("error", "Unable to create a connection to the Tanium Server when calling the api for saved question.") 
        log("warning", "Connection errors are usually due to server being offline or an invalid server name being provided.")
        log("debug", f"Error thrown: {error}")
        return None
    
    except Exception as error:
        log("error", "An unexpected error occurred when attempting to get Saved Question results. Check debug log entries.")
        log("error", f"Exception Name: {type(error).__name__}")
        log("error", f"{error}")
        return None

    if request.ok:
        log("info", "valid request")
        try:
            saved_question_results = json.loads(request.text)
        except json.JSONDecodeError as error:
            log("error", "A JSONDecodeError was thrown when parsing the request for the Saved Question Results on the Tanium Server")
            log("debug", f"The Saved Question ID being searched: {id}.")
            log("debug", f"JSONDecodeError message: {error.msg}")
            log("debug", f"Error was thrown processing the following: {error.doc}")
            log("debug", f"Error started as position: {error.pos}, on line {error.lineno}, column: {error.colno}")

        return saved_question_results['data']
    
    log("error", "Error getting saved question results")
    print("Error getting saved question results, quitting... ")
    quit()
    
def get_asset_view_results(id:str) -> dict:
    """Return results for an asset view from the Tanium Server"""
    params = {
        'viewId': id,
        'limit': 10_000_000
    }
    try:
        request = requests.get(f"https://{TANIUM_SERVER}/plugin/products/asset/v1/assets", headers = {'session': TANIUM_TOKEN}, params=params)

    except requests.exceptions.HTTPError as error:
        log("error", "A generic HTTPError was thrown when attempting to retrieve asset view results. Please make sure your server URL is correct, and that your server is online.")
        log("debug", f"Request Sent: {error.request}")
        log("debug", f"Response Text: {error.response.text}")
        return None

    except requests.exceptions.ConnectionError as error:
        log("error", "Unable to create a connection to the Tanium Server when calling the api for asset view results.") 
        log("warning", "Connection errors are usually due to server being offline or an invalid server name being provided.")
        log("debug", f"Error thrown: {error}")
        return None
    
    except Exception as error:
        log("error", "An unexpected error occurred when attempting to get Asset View results. Check debug log entries.")
        log("error", f"Exception Name: {type(error).__name__}")
        log("error", f"{error}")
        return None

    if request.ok:
        log("info", "valid request")
        try:
            asset_view_results = json.loads(request.text)
        except json.JSONDecodeError as error:
            log("error", "A JSONDecodeError was thrown when parsing the request for the Asset View Results on the Tanium Server")
            log("debug", f"The View Asset ID being searched: {id}.")
            log("debug", f"JSONDecodeError message: {error.msg}")
            log("debug", f"Error was thrown processing the following: {error.doc}")
            log("debug", f"Error started as position: {error.pos}, on line {error.lineno}, column: {error.colno}")

        return asset_view_results['data']
    
    log("error", "Error getting asset view results")
    print(request.reason)
    print(request.status_code)
    print("Error getting asset view results, quitting... ")
    quit()

def send_asset_report_to_splunk(asset_report_data: dict) -> requests.Response:
    """Sends asset report data to Splunk"""
    header = {'Authorization': SPLUNK_TOKEN}

    try: 
        return requests.post(f'https://{SPLUNK_SERVER}/services/collector/raw', headers=header, data = asset_report_data, verify=False)
    
    except requests.exceptions.HTTPError as error:
        log("error", "A generic HTTPError was thrown when attempting to query asset report to Splunk. Please make sure your server URL is correct, and that your server is online.")
        log("debug", f"Request Sent: {error.request}")
        log("debug", f"Response Text: {error.response.text}")
        return None

    except requests.exceptions.ConnectionError as error:
        log("error", "Unable to create a connection to the Tanium Server when calling the api to query asset reports.") 
        log("warning", "Connection errors are usually due to server being offline or an invalid server name being provided.")
        log("debug", f"Error thrown: {error}")
        return None
    
    except Exception as error:
        log("error", "An unexpected error occurred when attempting to query asset report to Splunk. Check debug log entries.")
        log("debug", f"Exception Name: {type(error).__name__}")
        log("debug", "Troubleshooting will need to be done to find the source of the error and a specific error handler should be made. ")
        return None
    
def send_asset_view_to_splunk(asset_view_data: str) -> requests.Response:
    """Sends asset view data to Splunk"""
    header = {'Authorization': SPLUNK_TOKEN}

    try: 
        return requests.post(f'https://{SPLUNK_SERVER}/services/collector/raw', headers=header, data = asset_view_data, verify=False)
    
    except requests.exceptions.HTTPError as error:
        log("error", "A generic HTTPError was thrown when attempting to query asset view data to Splunk. Please make sure your server URL is correct, and that your server is online.")
        log("debug", f"Request Sent: {error.request}")
        log("debug", f"Response Text: {error.response.text}")
        return None

    except requests.exceptions.ConnectionError as error:
        log("error", "Unable to create a connection to the Tanium Server when calling the api to query asset view data.") 
        log("warning", "Connection errors are usually due to server being offline or an invalid server name being provided.")
        log("debug", f"Error thrown: {error}")
        return None
    
    except Exception as error:
        log("error", "An unexpected error occurred when attempting to query asset view data to Splunk. Check debug log entries.")
        log("debug", f"Exception Name: {type(error).__name__}")
        log("debug", "Troubleshooting will need to be done to find the source of the error and a specific error handler should be made. ")
        return None
    
def send_saved_questions_to_splunk(saved_questions_data: str) -> requests.Response:
    """Sends saved question to Splunk"""
    header = {'Authorization': SPLUNK_TOKEN}

    try: 
        return requests.post(f'https://{SPLUNK_SERVER}/services/collector/raw', headers=header, data = saved_questions_data, verify=False)
    
    except requests.exceptions.HTTPError as error:
        log("error", "A generic HTTPError was thrown when attempting to query saved questions data to Splunk. Please make sure your server URL is correct, and that your server is online.")
        log("debug", f"Request Sent: {error.request}")
        log("debug", f"Response Text: {error.response.text}")
        return None

    except requests.exceptions.ConnectionError as error:
        log("error", "Unable to create a connection to the Tanium Server when calling the api to query saved questions data.") 
        log("warning", "Connection errors are usually due to server being offline or an invalid server name being provided.")
        log("debug", f"Error thrown: {error}")
        return None
    
    except Exception as error:
        log("error", "An unexpected error occurred when attempting to query saved questions data to Splunk. Check debug log entries.")
        log("debug", f"Exception Name: {type(error).__name__}")
        log("debug", "Troubleshooting will need to be done to find the source of the error and a specific error handler should be made. ")
        return None  
    
# FILE WRITING FUNCTIONS
def export_to_json(data: dict, destination: str) -> None:
    """Takes a dictionary and exports it to a json file on the local drive"""
    with open(destination, "w") as f:
        log("info", f"writing data to json file to {destination}")
        log("debug", f"data: {data}")

        try:
            json.dump(data, f)

        except Exception as error:
            log("error", "An unexpected error occurred when attempting to write the json data to a file. Check debug log entries.")
            log("debug", f"Exception Name: {type(error).__name__}")
            log("debug", "Troubleshooting will need to be done to find the source of the error and a specific error handler should be made.")

    return

def export_saved_question_results_to_csv(results:dict, filename:str):
    """Takes a saved question object from Tanium and writes it to a csv file on the local disk"""
    fieldnames = [ x['name'] for x in results['result_sets'][0]['columns'] ]

    with open(filename, "w") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()

        for row in results['result_sets'][0]['rows']:
            output = {}
            for index, text in enumerate(row['data']):
                output[fieldnames[index]] = text[0]['text']

            writer.writerow(output)

    csv_file.close()

def export_asset_report_results_to_csv(results:dict, filename:str):
    """Takes a asset report from Tanium and writes it to a csv file on the local disk"""
    fieldnames = [ x['displayName'] for x in results['columns'] ]
    
    with open(filename, "w") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        for row in results['rows']:
            output = {}
            for index, text in enumerate(row.values()):
                output[fieldnames[index]] = text

            writer.writerow(output)

    csv_file.close()

def export_asset_view_results_to_csv(data:dict, filename:str):
    """Takes a asset view from Tanium and writes it to a csv file on the local disk"""
    results = data['results']
    view    = data['view']
    fieldnames = [ x['displayName'] for x in view['definition']['attributes'] ]
    
    with open(filename, "w") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        for row in results:
            output = {}
            for index, text in enumerate(row.values()):
                output[fieldnames[index]] = text

            writer.writerow(output)

    csv_file.close()

def export_asset_view_results_flattened_to_csv(data:dict, filename:str):
    """Takes a asset view from Tanium and writes it to a csv file on the local disk"""
    results = data['results']
    view    = data['view']

    fieldnames = [ x['fieldName'] if x['tableName'] == 'ci_item' else f"{x['tableName']} {x['fieldName']}" for x in view['definition']['attributes'] ]

    with open(filename, "w") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()

        #About to write a nested mess. Refactor later
        if len([True for x in fieldnames if len(x.split(' ')) == 2]) > 0:
            for result_entry in results:
                largest_list_length = flatten_get_largest_list(result_entry)
                csv_rows = list([{fieldname: "" for fieldname in fieldnames}] * largest_list_length)
                
                for fieldname in fieldnames:
                    if len(fieldname.split(' ')) == 2:
                        table_name, result_field_name = fieldname.split(' ')
                        if not result_entry[table_name]:
                            continue

                        for index in range(len(result_entry[table_name])):
                            csv_rows[index][fieldname] = result_entry[table_name][index][result_field_name]
                            
                        continue
                    
                    for row in csv_rows:
                        row[fieldname] = result_entry[fieldname]

                for row in csv_rows:
                    writer.writerow(row)

    csv_file.close()

def write_to_s3(bucket_name: str, file_path: str, local_file: dict) -> None:
    """Uses the boto3 library to write to an s3 bucket"""
    s3 = boto3.resource('s3')
    bucket = bucket_name
    s3.meta.client.upload_file(f"TEMP/{local_file.split('/')[-1]}", bucket, file_path)

# ROUTING FUNCTIONS
def retrieve_data(config: JobConfig) -> dict:
    """A routing script that will take the data type and name from the config.csv and get the appropriate data for the jobs"""
    if config.tanium_type == "report":
        log("info", "type: report")
        target_report = find_asset_report_by_name(config.tanium_component_name)

        if not target_report:
            log("error", "Under type report, no report was found")
            return None

        query_report = query_asset_report(target_report['id'])

        if not query_report:
            log("error", "Unable to gather report results")
            return None
        
        return query_report
    
    elif config.tanium_type == "view":
        target_view = get_asset_view_by_name(config.tanium_component_name)

        if not target_view:
            log("error", "Under type view, no view was found")
            return None
        
        query_view = get_asset_view_results(target_view['id'])

        if not query_view:
            log("error", "Unable to gather asset view results")
            return None
        
        return {'view': target_view, 'results': query_view}
        
    elif config.tanium_type == "question":
        question_id = get_saved_question_id_by_name(config.tanium_component_name)

        if not question_id:
            return None
        
        question_results = get_saved_question_results(question_id)

        if not question_results:
            return None
        
        return question_results

    else:
        log("warning", f"An invalid form of data was assigned to gather. Unable to retrieve {type} from Tanium Server. Currently only support 'asset' and 'question' ")

def generate_file(config: JobConfig, data: dict) -> str:
    if config.tanium_type == "report":
        if config.flatten != '' and config.file_format == "csv":
            export_asset_view_results_flattened_to_csv(data, config.file_location)
        if config.file_format == "csv":
            export_asset_report_results_to_csv(data, config.file_location)
        if config.file_format == "json":
            export_to_json(data, config.file_location)
    
    if config.tanium_type == "view":
        if config.file_format == "csv":
            export_asset_view_results_to_csv(data, config.file_location)
        if config.file_format == "json":
            export_to_json(data['results'], config.file_location)

    if config.tanium_type == "question": 
        if config.file_format == "csv":
            export_saved_question_results_to_csv(data, config.file_location)

        if config.file_format == "json":
            export_to_json(data, config.file_location)

def export_data(data: dict, config: JobConfig):
    """Routing function to generate the specified file and copy it to the specified location"""
    print(config.destination_type)
    if config.destination_type == "s3":
        temp_file_location = f"TEMP/{config.file_location.split('/')[-1]}"
        generate_file(config, data)
        log("info", "type: s3, writing to s3...")
        write_to_s3(config.bucket_name, config.file_location, temp_file_location)
        # Add delete local file for s3
    elif config.destination_type == "file":
        generate_file(config, data)
        log("info", "type: file, exporting the results...")
    elif config.destination_type == "splunk":
        if config.tanium_type == "view":
            send_asset_view_to_splunk(data)
        elif config.tanium_type == "report":
            send_asset_report_to_splunk(data)
        elif config.tanium_type == "question":
            send_saved_questions_to_splunk(data)

    else:
        log("warning", f"An invalid form of data was assigned to the destination type. Unable to export to {config.destination_type}. Currently only support 's3' and 'file' ")

if __name__ == '__main__':
    setup_boto()
    updated_entries = []
    with open(CONFIG_FILE, 'r') as file:
        reader = csv.DictReader(file)

        for row in reader:
            job_config = JobConfig(row)
            print(job_config)
            print(job_config.dump())
            last_run   = datetime.strptime(job_config.last_run, "%Y-%m-%d %H:%M:%S") if job_config.last_run else ""
            frequency  = timedelta(hours=int(job_config.frequency))
            if last_run == "" or datetime.now() - last_run >= frequency:

                data = retrieve_data(job_config)
                if not data:
                    log('warning', f"Data was not returned when requesting {row['Tanium Type']}: {row['Component Name']}")
                    updated_entries.append(row)
                    continue
        
                export_data(data, job_config)
                
                job_config.last_run = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            updated_entries.append( job_config.dump() )


    # Overwrite the CSV file with the updated values
    with open(CONFIG_FILE, 'w', newline='') as file:
        fieldnames = ['Name', 'Destination Type', 'File Location', 'Frequency', 'Last Run', 'Tanium Type', 'Component Name', 'File Format', 'Bucket Name', 'Flatten', 'Overwrite']
        
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for entry in updated_entries:
            writer.writerow(entry)