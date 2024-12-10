from pymongo import MongoClient
import pandas as pd
import json
import numpy as np
import os
from jsonschema import validate, ValidationError
import csv


uri = "mongodb+srv://daniellin31223:HwGMxusif3xabROS@dapp.hd2pj.mongodb.net/?retryWrites=true&w=majority&appName=dapp"

# Database setup
client = MongoClient(
    uri,
    tls=True,  # Ensure TLS/SSL is enabled
    serverSelectionTimeoutMS=5000  # Increase timeout if needed
)
db = client['dapp']
users_collection = db['users']
data_collection=db['data']
MetricMapping_collection=db['MetricMapping']
MetricSelection_collection=db['MetricSelection']
StakeholderWeights_collection=db['StakeholderWeights']

json_schema = {
    "type": "object",
    "properties": {
        "ISIN": {"type": "string"},
        "Company": {"type": "string"},
        "BBG Ticker": {"type": "string"},
        "Analyst": {"type": "string"},
        "Team": {"type": "string"},
        "Date reviewed": {"type": "string", "format": "date-time"},  # Use date-time format for ISO 8601
        "Sector": {"type": ["string", "null"]},
        "Industry": {"type": ["string", "null"]},
        "Country": {"type": ["string", "null"]},
        "Region": {"type": ["string", "null"]},
        "MarketCap": {"type": ["number", "null"]},
        "SustainEx": {"type": ["string", "null"]},
        "NZAF": {"type": ["string", "null"]},
        "MSCI": {"type": ["string", "null"]},
        "ENERGY_CONSUMP_INTEN_USD": {"type": ["number", "null"]},
        "WATER_WD_INTEN_RECENT": {"type": ["number", "null"]},
        "CARBON_EMISSIONS_SCOPE_12_INTEN": {"type": ["number", "null"]},
        "HAS_COMMITTED_TO_SBTI_TARGET": {"type": ["boolean", "null"]},
        "HAS_SBTI_APPROVED_TARGET": {"type": ["boolean", "null"]},
        "SUST_BIODIV_PROTECT_POL_VALUE": {"type": ["boolean", "null"]},
        "TARGET_SUMMARY_CUM_CHANGE_2030": {"type": ["number", "null"]},
        "SBTI_NET_ZERO_TARGET_STATUS": {"type": ["boolean", "null"]},
        "Average Employee Length of service": {"type": ["number", "null"]},
        "Microfinance Impact Investment": {"type": ["number", "null"]},
        "Supplier ESG training": {"type": ["boolean", "null"]}
    },
    "additionalProperties": False
}

document_schema = {
    "type": "object",
    "properties": {
        "ISIN": {"type": "string"},
        "Company": {"type": "string"},
        "BBG Ticker": {"type": "string"},
        "Analyst": {"type": "string"},
        "Team": {"type": "string"},
        "Date reviewed": {"type": "string", "format": "date-time"},  # Use date-time format for ISO 8601
        "Sector": {"type": "string"},
        "Industry": {"type": "string"},
        "Country": {"type": "string"},
        "Region": {"type": "string"},
        "MarketCap": {"type": ["number", "null"]},
        "SustainEx": {"type": ["string", "null"]},
        "NZAF": {"type": ["string", "null"]},
        "MSCI": {"type": ["string", "null"]},
        "ENERGY_CONSUMP_INTEN_USD": {"type": ["number", "null"]},
        "WATER_WD_INTEN_RECENT": {"type": ["number", "null"]},  # Allowing null
        "CARBON_EMISSIONS_SCOPE_12_INTEN": {"type": ["number","null"]},
        "HAS_COMMITTED_TO_SBTI_TARGET": {"type": ["boolean","null"]},
        "HAS_SBTI_APPROVED_TARGET": {"type": ["boolean","null"]},
        "SUST_BIODIV_PROTECT_POL_VALUE": {"type": ["boolean","null"]},
        "TARGET_SUMMARY_CUM_CHANGE_2030": {"type": ["number", "null"]},
        "SBTI_NET_ZERO_TARGET_STATUS": {"type": ["boolean","null"]},  
        "Average Employee Length of service": {"type": ["number", "null"]},
        "Microfinance Impact Investment": {"type": ["number", "null"]},
        "Supplier ESG training": {"type": ["boolean","null"]},
        "Summary": {"type": ["string", "null"]},
        "Conclusion_select": {"type": "array","items": {"type": "string"}},
        "commit_list": {"type": "array","items": {"type": "string"}},
    },
    "required": [
        "ISIN", "Company", "BBG Ticker", "Analyst", "Team", "Date reviewed", "Sector", "Industry",
        "Country", "Region", "MarketCap", "SustainEx", "NZAF", "MSCI",
        "ENERGY_CONSUMP_INTEN_USD", "WATER_WD_INTEN_RECENT",
        "CARBON_EMISSIONS_SCOPE_12_INTEN", "HAS_COMMITTED_TO_SBTI_TARGET",
        "HAS_SBTI_APPROVED_TARGET", "SUST_BIODIV_PROTECT_POL_VALUE",
        "TARGET_SUMMARY_CUM_CHANGE_2030", "SBTI_NET_ZERO_TARGET_STATUS",
        "Average Employee Length of service", "Microfinance Impact Investment",
        "Supplier ESG training", "Summary", "Conclusion_select", "commit_list"
    ],
    "additionalProperties": False
}

metrics_schema={
    "type": "object",
    "properties": {
        "MetricName": {"type": "string"},
        "MetricCode": {"type": "string"},
        "MetricDescription": {"type": "string"},
        "Units": {"type": "string"},
        "Source": {"type": "string"},
        "Type": {"type": "string"},
        "Scoring": {"type": "string"},
        "Format": {"type": "string"},
        "Stakeholder": {"type": "string"},
    },
    "additionalProperties": False
}

weights_schema={
    "type": "object",
    "properties": {
        "Stakeholder": {"type": "string"},
        "Industrials": {"type": "number"},
        "Energy": {"type": "number"},
        "Consumer Staples": {"type": "number"},
        "Consumer Discretion": {"type": "number"},
        "Utilities": {"type": "number"},
        "Financials": {"type": "number"},
        "Telecommunications": {"type": "number"},
        "Health Care": {"type": "number"},
        "Technology": {"type": "number"},
        "Basic Materials": {"type": "number"},
        "Real Estate": {"type": "number"},
    },
    "additionalProperties": False
}

def update_or_insert_data(documents):
    collection = db['data']
    print("start upload")
    # Ensure all required fields exist with default values if needed
    required_defaults = {
        "ISIN": "",
        "Company": "",
        "BBG Ticker": "",
        "Analyst": "",
        "Team": "",
        "Date reviewed": "2001.01.01 00:00:00 UTC",  # Use date-time format for ISO 8601
        "Sector": "",
        "Industry": "",
        "Country": "",
        "Region": "",
        "MarketCap": 0,
        "SustainEx": "",
        "NZAF": "",
        "MSCI": "",
        "ENERGY_CONSUMP_INTEN_USD": 0.0,
        "WATER_WD_INTEN_RECENT": 0,
        "CARBON_EMISSIONS_SCOPE_12_INTEN": 0,
        "HAS_COMMITTED_TO_SBTI_TARGET": False,
        "HAS_SBTI_APPROVED_TARGET": False,
        "SUST_BIODIV_PROTECT_POL_VALUE": False,
        "TARGET_SUMMARY_CUM_CHANGE_2030": 0.0,
        "SBTI_NET_ZERO_TARGET_STATUS": False,
        "Average Employee Length of service": 0.0,
        "Microfinance Impact Investment": 0.0,
        "Supplier ESG training": False,
        "Summary": '',
        "Conclusion_select": [],
        "commit_list": []
    }
    i = 0
    error_list=[]
    for entry in documents:
        try:
            # Fill in missing fields with their default values
            for field, default in required_defaults.items():
                if field not in entry:
                    entry[field] = default
            
            # Validate each document against the schema
            validate(instance=entry, schema=document_schema)
            
            # Use 'ISIN' as a unique identifier for upserting into the database
            unique_field = entry.get("ISIN")
            if unique_field:
                query = {'ISIN': unique_field}
                collection.update_one(query, {'$set': entry}, upsert=True)
            else:
                print("Document does not have an ISIN field:", entry)

        except ValidationError as e:
            error_list.append(f"Validation error in document {entry.get('ISIN', 'unknown')}: {e.message}")
            print(f"Validation error in document {entry.get('ISIN', 'unknown')}: {e.message}")
            i += 1
    if i == 0:
        print("Success upload")
    else:
        print("Failure upload", i)
    return error_list

def update_or_insert_metrics(documents):
    MetricMapping_collection = db['MetricMapping']
    MetricSelection_collection = db['MetricSelection']
    first_json_keys = ["MetricName", "MetricCode", "MetricDescription", "Units", "Source", "Type", "Scoring", "Format"]
    second_json_keys = ["MetricCode", "Stakeholder"]
    
    print("Start upload")
    
    MetricMapping_collection.drop()
    MetricSelection_collection.drop()
    
    try:
        MetricMapping_json = [{key: item[key] for key in first_json_keys if key in item} for item in documents]
        MetricSelection_json = [{key: item[key] for key in second_json_keys if key in item} for item in documents]

        for item in MetricMapping_json:
            MetricMapping_collection.update_one(
                {"MetricCode": item["MetricCode"]},  # Filter by unique key
                {"$set": item},                      # Update the data
                upsert=True                          # Insert if not found
            )

        for item in MetricSelection_json:
            MetricSelection_collection.update_one(
                {"MetricCode": item["MetricCode"]},  # Filter by unique key
                {"$set": item},                      # Update the data
                upsert=True                          # Insert if not found
            )
        print("Metric upload OK")
    except Exception as e:
        return e


def update_or_insert_weights(documents):
    StakeholderWeights_collection = db['StakeholderWeights']
    print("start upload")
    # Ensure all required fields exist with default values if needed
    try:
        for item in documents:
            StakeholderWeights_collection.update_one(
                {"Stakeholder": item["Stakeholder"]},  # Filter by unique key
                {"$set": item},                      # Update the data
                upsert=True                          # Insert if not found
            )
        print("Weight upload OK")
    except Exception as e:
        return e

def convert_and_validate_types_data(item):
    """ Convert string representations in item to appropriate types. """
    conversions = {
        "MarketCap": float,
        "ENERGY_CONSUMP_INTEN_USD": float,
        "WATER_WD_INTEN_RECENT": float,
        "CARBON_EMISSIONS_SCOPE_12_INTEN": float,
        "HAS_COMMITTED_TO_SBTI_TARGET": lambda x: bool(float(x)),
        "HAS_SBTI_APPROVED_TARGET": lambda x: bool(float(x)),
        "SUST_BIODIV_PROTECT_POL_VALUE": lambda x: bool(float(x)),
        "TARGET_SUMMARY_CUM_CHANGE_2030": float,
        "SBTI_NET_ZERO_TARGET_STATUS": lambda x: bool(float(x)),
        "Average Employee Length of service": float,
        "Microfinance Impact Investment": lambda x: float(x) if x else None,
        "Supplier ESG training": lambda x: bool(float(x))
    }
    
    for key, convert in conversions.items():
        try:
            value = item[key].strip('"')  # Strip quotes from the string
            if value == 'NA':
                item[key] = None
            elif value != '':
                item[key] = convert(value)
            else:
                item[key] = None
        except (ValueError, TypeError) as e:
            print(f"Type conversion error for {key} ({item[key]}): {e}")

    return item

def convert_and_validate_types_metrics(item):
    """ Convert string representations in item to appropriate types. """
    conversions = {
        "MetricName": str,
        "MetricCode": str,
        "MetricDescription": str,
        "Units": str,
        "Source": str,
        "Type": str,
        "Scoring": str,
        "Format": str,
        "Stakeholder": str,
    }

    for key, convert in conversions.items():
        try:
            # Check if the key exists in the item
            if key in item:
                value = item[key].strip('"')  # Strip quotes from the string
                if value == 'NA':  # Handle 'NA' as None
                    item[key] = None
                elif value != '':
                    item[key] = convert(value)
                else:
                    item[key] = None
            else:
                # If the key is missing, set it to None
                item[key] = None
        except (ValueError, TypeError) as e:
            print(f"Type conversion error for {key} ({item.get(key)}): {e}")
    print('item =>', item)
    return item


def convert_and_validate_types_weights(item):
    """ Convert string representations in item to appropriate types. """
    conversions = {
        "Stakeholder": str,
        "Industrials": float,
        "Energy": float,
        "Consumer Staples": float,
        "Consumer Discretion": float,
        "Utilities": float,
        "Financials": float,
        "Telecommunications": float,
        "Health Care": float,
        "Technology": float,
        "Basic Materials": float,
        "Real Estate": float,
    }

    for key, convert in conversions.items():
        try:
            value = item[key].strip('"')  # Strip quotes from the string
            if value == 'NA':
                item[key] = None
            elif value != '':
                item[key] = convert(value)
            else:
                item[key] = None
        except (ValueError, TypeError) as e:
            print(f"Type conversion error for {key} ({item[key]}): {e}")

    return item

def csv_to_json_data(csv_file_path):
    """ Converts a CSV file to a JSON file and validates it against a JSON schema. """
    json_file_path = "output.json"
    # Read CSV data
    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data = [convert_and_validate_types_data(row) for row in csv_reader]

    # Validate each item against the JSON schema
    validated_data = []
    for item in data:
        try:
            validate(instance=item, schema=json_schema)
            validated_data.append(item)
        except ValidationError as e:
            print(f"Validation error for item {item}: {e.message}")
            continue
    
    # Write validated data to JSON
    with open(json_file_path, mode='w', encoding='utf-8') as json_file:
        json.dump(validated_data, json_file, indent=4)

    print(f"Successfully converted and validated {csv_file_path} to {json_file_path}")
    return json_file_path

def csv_to_json_metrics(csv_file_path):
    """ Converts a CSV file to a JSON file and validates it against a JSON schema. """
    json_file_path = "output.json"
    # Read CSV data
    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data = [convert_and_validate_types_metrics(row) for row in csv_reader]

    # Validate each item against the JSON schema
    validated_data = []
    for item in data:
        try:
            validate(instance=item, schema=metrics_schema)
            validated_data.append(item)
        except ValidationError as e:
            print(f"Validation error for item {item}: {e.message}")
            continue
    
    # Write validated data to JSON
    with open(json_file_path, mode='w', encoding='utf-8') as json_file:
        json.dump(validated_data, json_file, indent=4)

    print(f"Successfully converted and validated {csv_file_path} to {json_file_path}")
    return json_file_path

def csv_to_json_weights(csv_file_path):
    """ Converts a CSV file to a JSON file and validates it against a JSON schema. """
    json_file_path = os.path.join(app.config['UPLOAD_FOLDER'], "output.json")
    # Read CSV data
    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data = [convert_and_validate_types_weights(row) for row in csv_reader]

    # Validate each item against the JSON schema
    validated_data = []
    for item in data:
        try:
            validate(instance=item, schema=weights_schema)
            validated_data.append(item)
        except ValidationError as e:
            print(f"Validation error for item {item}: {e.message}")
            continue
    
    # Write validated data to JSON
    with open(json_file_path, mode='w', encoding='utf-8') as json_file:
        json.dump(validated_data, json_file, indent=4)

    print(f"Successfully converted and validated {csv_file_path} to {json_file_path}")
    return json_file_path

file_path = './uploads/Metrics.csv'


    # Handle JSON files
if file_path.endswith('.json'):
    with open(file_path, 'r') as f:
        documents = json.load(f)

# Handle CSV or Excel files
elif file_path.endswith('.csv') or file_path.endswith(('.xls', '.xlsx')):
    json_path = csv_to_json_metrics(file_path)  # Convert CSV/Excel to JSON
    with open(json_path, 'r') as f:
        documents = json.load(f)

else:
    print({'error': 'Invalid file type. Only JSON, CSV, or Excel files are supported.'})

# Pass the documents directly to the function
errors = update_or_insert_metrics(documents)
print(errors)