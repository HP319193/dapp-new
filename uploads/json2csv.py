import csv
import json
import os

def json_to_csv(json_file_path, csv_file_path):
    """
    Converts a JSON file to a CSV file.
    """
    try:
        # Read the JSON data
        with open(json_file_path, mode='r', encoding='utf-8') as json_file:
            data = json.load(json_file)

        # Ensure the JSON contains data
        if not data:
            raise ValueError("The JSON file is empty.")

        # Extract headers from the keys of the first dictionary
        headers = data[0].keys()

        # Write data to CSV
        with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()  # Write CSV headers
            writer.writerows(data)  # Write data rows

        print(f"Successfully converted {json_file_path} to {csv_file_path}")

    except FileNotFoundError:
        print(f"Error: File {json_file_path} not found.")
    except ValueError as ve:
        print(f"Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Paths to JSON and CSV files
input_json_path = 'uploads/output.json'  # Replace with your JSON file path
output_csv_path = 'uploads/output.csv'  # Desired output CSV file path

# Convert JSON to CSV
json_to_csv(input_json_path, output_csv_path)
