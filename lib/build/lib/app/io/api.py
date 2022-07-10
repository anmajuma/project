import requests
import sys
import json
from app.config.helper import read_config

def call_api():

    config = read_config()
    apiUrl = config['APISettings']['url']
    api_app_id = config['APISettings']['app_id']
    api_app_key = config['APISettings']['app_key']
    url = apiUrl
    headers = {
      'Accept': 'application/json',
      'resourceversion': 'v4',
      'app_id': api_app_id,
      'app_key': api_app_key
      }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
              return(response.json())
    except requests.exceptions.ConnectionError as error:
        print(error)
        sys.exit()
