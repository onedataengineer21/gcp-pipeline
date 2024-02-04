import requests
import datetime
import random
import pandas as pd

def extract_app_users_data(filename: str, foldername: str) -> str:
    url = f'http://randomuser.me/api?results={random.randint(40,80)}&nat=us,gb,in,es,ca,au'
    result = requests.get(url)
    result.raise_for_status()   # raise an exception if bad response returned
    data = result.json()
    users = pd.json_normalize(data["results"], max_level=1)
    users = users[["gender","email","phone","name.title","name.first","name.last", "location.city","location.state","location.country", "dob.date", "dob.age"]]
    users.columns = ["Gender", "Email", "Phone", "Title", "First_Name", "Last_Name", "City", "State", "Country", "DOB", "Age"]
    users = users[["Title", "First_Name", "Last_Name", "Gender", "DOB", "Age", "Email", "Phone", "City", "State", "Country"]]
    users.to_parquet(f'gs://api-composer-gcs-app-user-data/dt={foldername}/{filename}')
    return f'We downloaded {len(users)} users data!'

