import requests
import random
import pandas as pd

def extract_app_users_data():
    """
    Pulling randomuser information using the API
    With each call, we try to pull only 40-80 records randomly.
    """
    url = f'http://randomuser.me/api?results={random.randint(40,80)}&nat=us,gb,in,es,ca,au'
    result = requests.get(url)
    result.raise_for_status()   # raise an exception if bad response returned
    data = result.json()
    users = pd.json_normalize(data["results"], max_level=1)
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
    This is the main method in the job which calls the modules for completing the job
    """
    try:
        ### Extracting the dataset from the API
        data = extract_app_users_data()

        ### Transforming the dataset
        transformed = transform(data)

        ### Loading the dataset to storage in parquet format
        load_data_storage(transformed, foldername, filename)

    except Exception as e:
            print(f"ERROR IN THE USER DATA EXTRACTION JOB: {e}")
            return -1

    return None