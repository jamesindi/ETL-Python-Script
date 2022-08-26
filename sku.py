# Built-in packages
import requests
import json
import hashlib
from datetime import datetime

# External Packages
from google.cloud import bigquery
from google.oauth2 import service_account
import requests

# Extract

url = "https://www.jakmall.com/top-100?category=Handphone-tablet&payloads=\" \""
SA_CREDENTIALS_FILE = 'credentials-kelompok-1.json' 
payload={}
headers = {
  'Accept': 'application/json',
  'Cookie': 'XSRF-TOKEN=eyJpdiI6IkF3RDFlQkZmWTB3NWtKN1dPOVwvVGhRPT0iLCJ2YWx1ZSI6IlBHYU1pdFBrc0pTejFBa2UrQ1JUeDFmNVwvM2VjNzNlNEVPMFFFWThQZFErQWhFZFplSzQ2d01yVUp3SmRzVVZLIiwibWFjIjoiYTA3MzU0MGFlMDQyZjBkZGZlZTNlM2JiNzQwODQ0ZjJjYTQ1MWQ1OWMwODUwMjZiMjBlMGU3ZDdlN2E0NzE3ZSJ9; jsi=eyJpdiI6Ind2cEh0QjhMYTRVV0lITHN0MXdwelE9PSIsInZhbHVlIjoiN2RuVjRkU3FBQWxmc0RjcGtRY0VINE44eU5Ub2Y4ZkdEWUZuYzRzT09OZGQyYjZvUWlmRlpMXC9rbWZIWHZ2UlIiLCJtYWMiOiJjMTEyZmNjNTJkZTkzNTU3ZDQzN2U4NTI4YzI3ZDI4MzNhYzIwMzMyZmU4MjMxYzI4MzRlYjg1MWEyYzBmYWFmIn0%3D; AWSALB=ATvmfJqjxa2RJnFF3oh4B+VanpS/KtDmygRGu+XP0iDlTxYeadPZDTCv7EcEBcnejJjyDpM0RO0AKcZGnXbYXCLgjrwv4APDaBsQ59MZtIUUeg3kowPq7FyAjWfh; AWSALBCORS=ATvmfJqjxa2RJnFF3oh4B+VanpS/KtDmygRGu+XP0iDlTxYeadPZDTCv7EcEBcnejJjyDpM0RO0AKcZGnXbYXCLgjrwv4APDaBsQ59MZtIUUeg3kowPq7FyAjWfh'
}

response = requests.request("GET", url, headers=headers, data=payload)

#print(response.text)
data=json.loads(response.text)
#print(type(data['products'])) cek type


# Transformed


def transformed_sku(data):
    sku=[]
    sku2=[]
    i = 0
    
    for item in data['products']:
        str=data['products'][i]['code']
        plu=data['products'][i]['sku'][0]
        sku.append(
            {
            'super_key': hashlib.md5(str.encode()).hexdigest(),  
            'data_sku': json.dumps(plu),
            'time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")   
            
            #'sku2': json.dumps(plu)
        
              }
        )        
        i += 1
        
    print("berhasil transform sku")
    return sku

def load(transformed_data, table_id):
    """Memasukkan data yang sudah di transform ke table database -> BigQuery.
    """
    credential = service_account.Credentials.from_service_account_file(
            SA_CREDENTIALS_FILE,
        )

    client = bigquery.Client(
        credentials=credential,
        project=credential.project_id,
    )
    
    client.insert_rows_json(table_id, transformed_data)
    
    print("Berhasil")


#final
transformed_data=transformed_sku(data)
table_id = 'binar-bie7.kelompok_1_stg.sku_raw_baru'
load(transformed_data, table_id)
