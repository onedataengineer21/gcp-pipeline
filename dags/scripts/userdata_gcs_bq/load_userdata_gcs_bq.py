import datetime
from google.cloud import bigquery
import pytz
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
    print("transform completed")
    return data

def load_data_storage(data, config, foldername, filename):
    """
    Writing the dataset to the cloud storage in the parquet format
    """
    bucketname = config['gcs_settings']['gcs_bucket_name']
    data.to_parquet(f'{bucketname}/dt={foldername}/{filename}')
    return None

def load_data_bq(data):
    """
    Writing the dataset to the cloud storage in the parquet format
    """
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = config['bq_settings']['bq_table_id']

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Title", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("First_Name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Last_Name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Gender", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("DOB", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Age", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Email", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Phone", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("City", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("State", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Country", bigquery.enums.SqlTypeNames.STRING),
        ]
    )

    job = client.load_table_from_dataframe(
        data, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    print("load_data_bq completed")
    return None

def app(filename, foldername):
    """
    This is the main method in the job which calls the modules for completing the job
    """
    try:
        config = load_config()

        ### Extracting the dataset from the API
        data = extract_app_users_data(config)

        ### Transforming the dataset
        transformed = transform(data)
        transformed = transformed.astype('str')

        ### Loading the dataset to storage in parquet format
        load_data_storage(transformed, config, foldername, filename)

        ### Loading the dataset to bq
        load_data_bq(transformed, config)

    except Exception as e:
            print(f"ERROR IN THE USER DATA EXTRACTION JOB: {e}")
            return -1

    return None
