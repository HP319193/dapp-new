# File: app.py

from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from pymongo import MongoClient
import pandas as pd
import json
import numpy as np

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
        return redirect(url_for('main'))
    return "Login Failed", 401

@app.route('/main')
def main():
    if 'user' not in session:
        return redirect(url_for('sign_in'))
    return render_template('main.html')

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('sign_in'))

@app.route('/dashboard', methods = ['GET'])
def dashboard():
    company_name_list=all_data['Company']
    return render_template('dashboard.html', data=dashboard_json(all_data['Company'][0]), company_name_list=company_name_list, select_list=select_list)

@app.route('/dashboard_update', methods=['POST'])
def update_dashboard():
    data = request.get_json()
    print('data', data)
    dashboard_data = json.dumps(dashboard_json(data['value']),indent=4)

    return jsonify(data=dashboard_data, select_list=select_list)

@app.route('/dashboard_save', methods=['POST'])
def save_dashboard():
    data = request.get_json()
    print("data", data)
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
    return "OK"

@app.route('/metrics')
def metrics():
    return render_template('metrics.html')

@app.route('/users')
def users():
    # Mocked session for testing; replace with real login logic
    session['is_admin'] = True  # Set to False for non-admin users

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
        print(conclusion)
        stakeholder_info.update({
            'Calculated score': calculated_average,
            'Comment': comment,
            'Conclusion': conclusion,
        })

        json_structure['middle'].append(stakeholder_info)
    print(json_structure)
    return json_structure



if __name__ == '__main__':
    app.run()
