import csv
import json
from jsonschema import validate, ValidationError

def convert_and_validate_types(item):
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

def csv_to_json(csv_file_path, json_file_path, json_schema):
    """ Converts a CSV file to a JSON file and validates it against a JSON schema. """
    try:
        # Read CSV data
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            data = [convert_and_validate_types(row) for row in csv_reader]

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

    except FileNotFoundError:
        print(f"Error: File {csv_file_path} not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Example JSON schema with optional fields
json_schema = {
    "type": "object",
    "properties": {
        "ISIN": {"type": "string"},
        "Company": {"type": "string"},
        "BBG Ticker": {"type": "string"},
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

# Update the paths below to your CSV and desired JSON output file paths.
csv_to_json('uploads/ESGDataUI2910.csv', 'uploads/output.json', json_schema)
