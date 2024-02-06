import requests
import random
import pandas as pd
import yaml

def load_config(config_path='configs/pipeline.yaml'):
    """Load the pipeline configuration from a YAML file."""
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def extract_app_users_data(config):
    """
    Pulling randomuser information using the API
    With each call, we try to pull only 40-80 records randomly.
    """
    url = config['api_settings']['api_path']
    result = requests.get(f'{url}?results={random.randint(40,80)}&nat=us,gb,in,es,ca,au')
    result.raise_for_status()   # raise an exception if bad response returned
    data = result.json()
    users = pd.json_normalize(data["results"], max_level=1)
    print("extract_app_users_data completed")
    return users

def transform(data):
    """
    Selecting only the required columns from the source, renaming the columns and ordering the columns
    """
    data = data[["gender","email","phone","name.title","name.first","name.last", "location.city","location.state","location.country", "dob.date", "dob.age"]]
    data.columns = ["Gender", "Email", "Phone", "Title", "First_Name", "Last_Name", "City", "State", "Country", "DOB", "Age"]
    data = data[["Title", "First_Name", "Last_Name", "Gender", "DOB", "Age", "Email", "Phone", "City", "State", "Country"]]
    return data

def load_data_storage(data, foldername, filename):
    """
    Writing the dataset to the cloud storage in the parquet format
    """
    data.to_parquet(f'gs://api-composer-gcs-app-user-data/dt={foldername}/{filename}')
    return None

def app(filename, foldername):
    """
    This is the main method which calls the modules for completing the job
    """
    try:
        config = load_config()

        ### Extracting the dataset from the API
        data = extract_app_users_data(config)

        ### Transforming the dataset
        transformed = transform(data)

        ### Loading the dataset to storage in parquet format
        load_data_storage(transformed, foldername, filename)

    except Exception as e:
            print(f"ERROR IN THE USER DATA EXTRACTION JOB: {e}")
            return -1

    return None
