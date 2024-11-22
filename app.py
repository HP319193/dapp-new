# File: app.py

from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from pymongo import MongoClient
import pandas as pd
import json
import numpy as np
import os
from jsonschema import validate, ValidationError
import csv


app = Flask(__name__)
app.secret_key = 'supersecretkey'

# Database setup
client = MongoClient("mongodb+srv://daniellin31223:HwGMxusif3xabROS@dapp.hd2pj.mongodb.net/?retryWrites=true&w=majority&appName=dapp")
db = client['dapp']
users_collection = db['users']
data_collection=db['data']
MetricMapping_collection=db['MetricMapping']
MetricSelection_collection=db['MetricSelection']
StakeholderWeights_collection=db['StakeholderWeights']

# Configure upload folder
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

all_users = pd.DataFrame(list(users_collection.find()))
all_data = pd.DataFrame(list(data_collection.find()))
all_MetricMapping = pd.DataFrame(list(MetricMapping_collection.find()))
all_MetricSelection = pd.DataFrame(list(MetricSelection_collection.find()))
all_StakeholderWeights = pd.DataFrame(list(StakeholderWeights_collection.find()))
# print(all_MetricSelection)
Stakeholder_list = all_StakeholderWeights['Stakeholder'].unique()
select_list = ['Very weak', 'Weak', 'Medium','Strong', 'Very strong']
score = {
    "Min":{
        'Very weak':0,
        'Weak':0.2,
        'Medium':0.4,
        'Strong':0.6,
        'Very strong':0.8
    },
    "Mid":{
        'Very weak':0.1,
        'Weak':0.3,
        'Medium':0.5,
        'Strong':0.7,
        'Very strong':0.9
    },
    "Max":{
        'Very weak':0.2,
        'Weak':0.4,
        'Medium':0.6,
        'Strong':0.8,
        'Very strong':1
    },
}

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

document_schema = {
    "type": "object",
    "properties": {
        "ISIN": {"type": "string"},
        "Company": {"type": "string"},
        "BBG Ticker": {"type": "string"},
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
        "ISIN", "Company", "BBG Ticker", "Sector", "Industry",
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

# Routes
@app.route('/')
def sign_in():
    if 'user' in session:
        return redirect(url_for('main'))
    return render_template('sign_in.html')

@app.route('/login', methods=['POST'])
def login():
    username = request.form['username']
    password = request.form['password']
    user = users_collection.find_one({"username": username, "password": password})
    if user:
        session['user'] = username
        return redirect(url_for('dashboard'))
    return "Login Failed", 401

@app.route('/main')
def main():
    if 'user' not in session:
        return redirect(url_for('sign_in'))
    # Redirect to dashboard or call the function and return its result
    return dashboard()

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('sign_in'))

@app.route('/dashboard', methods = ['GET'])
def dashboard():
    company_name_list=all_data['Company']
    return render_template(
        'dashboard.html', 
        data=dashboard_json(all_data['Company'].iloc[0]), 
        company_name_list=company_name_list, 
        select_list=select_list
    )

@app.route('/dashboard_update', methods=['POST'])
def update_dashboard():
    data = request.get_json()
    # print('data', data)
    dashboard_data = json.dumps(dashboard_json(data['value']),indent=4)

    return jsonify(data=dashboard_data, select_list=select_list)

@app.route('/dashboard_save', methods=['POST'])
def save_dashboard():
    data = request.get_json()
    # print("data", data)
    filter_query = {'Company': data['Company']}
    update_data = {
        '$set': {
            'Summary': data['Summary'],
            'commit_list': data['commit_list'],
            'Conclusion_select': data['Conclusion_select']
        }
    }
    result = data_collection.update_one(filter_query, update_data)
    if result.matched_count > 0:
        print(f"Successfully updated the document for Company: {data['Company']}")
    else:
        print(f"No document found for Company: {data['Company']}")
    return jsonify({"message":"File save Success"})

@app.route('/metrics', methods=['GET'])
def metrics():
    # Convert stakeholder weights to dictionary and remove _id
    stakeholder_weights_data = all_StakeholderWeights.to_dict(orient='records')
    for item in stakeholder_weights_data:
        item.pop('_id', None)

    print(stakeholder_weights_data)
    
    # Select necessary columns from MetricSelection
    metric_selection_cleaned = all_MetricSelection.loc[:, ['Stakeholder', 'MetricCode']]
    
    # Merge with MetricMapping
    integrated_table = pd.merge(all_MetricMapping, metric_selection_cleaned, on='MetricCode', how='inner')
    integrated_table_data = integrated_table.to_dict(orient='records')
    
    # Remove _id from merged data
    for item in integrated_table_data:
        item.pop('_id', None)
        
    print(integrated_table_data)
    
    return render_template('metrics.html', stakeholder_weights=stakeholder_weights_data, integrated_table=integrated_table_data)


@app.route('/save-metrics', methods=['POST'])
def save_metrics():
    data = request.get_json()
    first_json_keys = ["MetricName", "MetricCode", "MetricDescription", "Units", "Source", "Type", "Scoring", "Format"]
    second_json_keys = ["MetricCode", "Stakeholder"]

    # Split data
    try:
        MetricMapping_json = [{key: item[key] for key in first_json_keys if key in item} for item in data]
        MetricSelection_json = [{key: item[key] for key in second_json_keys if key in item} for item in data]

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
        return jsonify({'message': 'OK'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/save-weight', methods=['POST'])
def save_weight():
    data = request.get_json()
    try:
        for item in data:
            MetricSelection_collection.update_one(
                {"Stakeholder": item["Stakeholder"]},  # Filter by unique key
                {"$set": item},                      # Update the data
                upsert=True                          # Insert if not found
            )
        print("Weight upload OK")
        return jsonify({'message': 'OK'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(file_path)

    try:
        # Handle JSON files
        if file.filename.endswith('.json'):
            with open(file_path, 'r') as f:
                documents = json.load(f)

        # Handle CSV or Excel files
        elif file.filename.endswith('.csv') or file.filename.endswith(('.xls', '.xlsx')):
            json_path = csv_to_json(file_path)  # Convert CSV/Excel to JSON
            with open(json_path, 'r') as f:
                documents = json.load(f)

        else:
            return jsonify({'error': 'Invalid file type. Only JSON, CSV, or Excel files are supported.'}), 400

        # Pass the documents directly to the function
        errors = update_or_insert(documents)

        if len(errors) > 0:
            return jsonify({'message': 'File processed with errors', 'errors': len(errors)}), 200
        else:
            return jsonify({'message': 'File processed successfully'}), 200

    except json.JSONDecodeError:
        return jsonify({'error': 'Invalid JSON format'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/users')
def users():
    return render_template('users.html', is_admin=session.get('is_admin', False))

@app.route('/get_users', methods=['GET'])
def get_users():
    users = list(users_collection.find({}, {'_id': 0, 'username': 1, 'password': 1, 'is_admin': 1}))
    for user in users:
        user['password'] = "*****"  # Hide actual password
    return jsonify(users)

@app.route('/create_user', methods=['POST'])
def create_user():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    confirm = data.get('confirm')

    if password != confirm:
        return jsonify({'success': False, 'message': "Password and confirm password don't match."})

    if len(password)<8:
        return jsonify({'success': False, 'message': "length of password must be over 8 letters"})
    
    if users_collection.find_one({'username': username}):
        return jsonify({'success': False, 'message': "Username already exists. Use another username."})

    users_collection.insert_one({'username': username, 'password': password, 'is_admin': False})
    return jsonify({'success': True, 'message': "User created successfully."})

def dashboard_json(company_name):
    company_entry = all_data[all_data['Company'] == company_name].iloc[0]  # Select first row as an example

    json_structure = {
        'Company': company_entry['Company'],
        'Sector': company_entry['Sector'],
        'Analyst': "Analyst",
        'Date reviewed': "Data reviewed",
        'ISIN': company_entry['ISIN'],
        'Region': company_entry['Region'],
        'Team': "Team",
        'BBG Ticker': company_entry['BBG Ticker'],
        'Summary': company_entry['Summary'],
        'middle': []
    }
    
    for stakeholder in Stakeholder_list:
        weight_str = all_StakeholderWeights.loc[
            all_StakeholderWeights['Stakeholder'] == stakeholder, company_entry['Sector']
        ].values[0]
        if isinstance(weight_str, str) and weight_str.endswith('%'):
            weight = float(weight_str.strip('%')) / 100
        else:
            weight = float(weight_str)

        stakeholder_info = {
            'Category': stakeholder,
            'Weight': weight,
            'value': []
        }

        metric_codes = all_MetricSelection.loc[all_MetricSelection['Stakeholder'] == stakeholder, 'MetricCode'].tolist()
        Calculated_score = []

        for MetricCode in metric_codes:
            filtered_df = all_MetricMapping.loc[all_MetricMapping["MetricCode"] == MetricCode]
            if not filtered_df.empty:
                company_value = company_entry[MetricCode]
                # print('company-value :', company_value)
                if isinstance(company_value, float) and np.isnan(company_value):
                    company_value = 0
                elif isinstance(company_value, (np.int64, np.float64)):
                    company_value = float(company_value)
                elif isinstance(company_value, np.bool_):
                    company_value = bool(company_value)
                else:
                    company_value = 0

                sector_value = float(all_data.loc[all_data['Sector'] == company_entry['Sector'], MetricCode].mean())
                region_value = float(all_data.loc[all_data['Region'] == company_entry['Region'], MetricCode].mean())
                global_value = float(all_data[MetricCode].mean())
                if isinstance(sector_value, float) and np.isnan(sector_value):
                    sector_value = 0
                if isinstance(region_value, float) and np.isnan(region_value):
                    region_value = 0
                if isinstance(global_value, float) and np.isnan(global_value):
                    global_value = 0

                # print("company", company_value)

                metric_info = {
                    'Metric': filtered_df.iloc[0]['MetricName'],
                    'Description': filtered_df.iloc[0]['MetricDescription'],
                    'Units': filtered_df.iloc[0]['Units'],
                    'Source': filtered_df.iloc[0]['Source'],
                    'Company': company_value,
                    'Sector': sector_value,
                    'Region': region_value,
                    'Global': global_value
                }
                stakeholder_info['value'].append(metric_info)

                if 'If Y' == filtered_df.iloc[0]['Scoring']:
                    Calculated_score.append(1 if company_value else 0)
                elif 'If N' == filtered_df.iloc[0]['Scoring']:
                    Calculated_score.append(1 if not company_value else 0)
                elif 'Higher than' in filtered_df.iloc[0]['Scoring']:
                    if 'sector' in filtered_df.iloc[0]['Scoring']:
                        Calculated_score.append(1 if company_value > sector_value else 0)
                    elif 'global' in filtered_df.iloc[0]['Scoring']:
                        Calculated_score.append(1 if company_value > global_value else 0)
                    elif 'regional' in filtered_df.iloc[0]['Scoring']:
                        Calculated_score.append(1 if company_value > region_value else 0)
                elif 'Lower than' in filtered_df.iloc[0]['Scoring']:
                    if 'sector' in filtered_df.iloc[0]['Scoring']:
                        Calculated_score.append(1 if company_value < sector_value else 0)
                    elif 'global' in filtered_df.iloc[0]['Scoring']:
                        Calculated_score.append(1 if company_value < global_value else 0)
                    elif 'regional' in filtered_df.iloc[0]['Scoring']:
                        Calculated_score.append(1 if company_value < region_value else 0)

        if Calculated_score:
            calculated_average = sum(Calculated_score) / len(Calculated_score)
        else:
            calculated_average = None
        if stakeholder in company_entry['commit_list']:
            comment = company_entry['commit_list'][stakeholder]
        else:
            comment = ""
        if stakeholder in company_entry['Conclusion_select']:
            conclusion = company_entry['Conclusion_select'][stakeholder]
        else:
            conclusion = select_list[0]
        # print(conclusion)
        stakeholder_info.update({
            'Calculated score': calculated_average,
            'Comment': comment,
            'Conclusion': conclusion,
        })

        json_structure['middle'].append(stakeholder_info)
    # print(json_structure)
    return json_structure


def update_or_insert(documents):
    collection = db['data']
    print("start upload")
    # Ensure all required fields exist with default values if needed
    required_defaults = {
        "ISIN": "",
        "Company": "",
        "BBG Ticker": "",
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

def csv_to_json(csv_file_path):
    """ Converts a CSV file to a JSON file and validates it against a JSON schema. """
    json_file_path = os.path.join(app.config['UPLOAD_FOLDER'], "output.json")
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
    return json_file_path


if __name__ == '__main__':
    app.run()
