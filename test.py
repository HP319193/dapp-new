import json
import numpy as np

# Your JSON data as a string
dashboard_data = '''
{
    "Company": "S&P Global Inc.",
    "Sector": "Financials",
    "Analyst": NaN,
    "Date reviewed": "Invalid Timestamp",
    "ISIN": "US78409V1044",
    "Region": "N America",
    "Team": NaN,
    "BBG Ticker": "US78409V1044",
    "Summary": "",
    "middle": [
        {
            "Category": "Environment",
            "Weight": 0.2,
            "value": [
                {
                    "Metric": "Water intensity",
                    "Description": "Water withdrawal normalised by $1m of sales",
                    "Units": "m3/$m sales",
                    "Source": "MSCI",
                    "Company": 5.46,
                    "Sector": 2562.17,
                    "Region": 44425.68,
                    "Global": 124042.90
                }
            ],
            "Calculated score": 0.5,
            "Comment": "",
            "Conclusion": "Very weak"
        }
    ]
}
'''

# Replace NaN with None for proper JSON parsing
dashboard_data = dashboard_data.replace('NaN', 'null')

# Load the JSON data into a Python dictionary
data_dict = json.loads(dashboard_data)

# Define a function to replace None values with empty strings
def replace_none_with_empty_string(value):
    if isinstance(value, dict):
        return {k: replace_none_with_empty_string(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [replace_none_with_empty_string(item) for item in value]
    else:
        return value

# Clean the data
cleaned_data = replace_none_with_empty_string(data_dict)

# Print the cleaned data
print(json.dumps(cleaned_data, indent=4))