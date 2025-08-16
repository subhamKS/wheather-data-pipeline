import requests
import json
import os
from datetime import datetime
from pathlib import Path

#from config import dev

API_KEY = os.getenv("WEATHER_API_KEY")

"""
Buinding the path to the config file, first we get the directory of the current file
then we go up one level to the parent directory(using '..' like cd ..), and then into the config folder
and finally we specify the config file name
Accorging to the os linux, mac or windows the path will be built accordingly(/ or \ or \\)
"""

if not API_KEY:
    config_path = os.path.join(os.path.dirname(__file__), '..','config','secrets.json')
    with open(config_path, 'r') as reader:
        config = json.load(reader)
    API_KEY = config['api_key']

config_path = os.path.join(os.path.dirname(__file__), '..','config','dev.json')

with open(config_path, 'r') as reader:
    config = json.load(reader)
cities = config['cities']
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

output_dir = Path(os.path.join(os.path.dirname(__file__), '..', 'data','raw'))
today = datetime.utcnow().strftime('%Y-%m-%d')
partition_dir = output_dir/ f"date = {today}"
partition_dir.mkdir(parents=True, exist_ok=True)

timestamp = datetime.utcnow().strftime("%H%M")
file_path = partition_dir / f"weather_{timestamp}.jsonl"

#cities = config['cities'][0:3]  # Example city, you can loop through all cities

with open(file_path, "w") as f:
    for city in cities:
        params = {"q": city, "appid": API_KEY, "units": "metric"}
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            f.write(json.dumps(data) + "\n")  # newline-delimited JSON
        else:
            print(f"Failed for {city}: {response.status_code} - {response.text}")