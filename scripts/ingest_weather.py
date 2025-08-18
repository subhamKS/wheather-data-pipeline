import requests
import json
import os
from datetime import datetime
import boto3
from io import StringIO

# API Key (from env var or config)
API_KEY = os.getenv("WEATHER_API_KEY")
if not API_KEY:
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'secrets.json')
    with open(config_path, 'r') as reader:
        config = json.load(reader)
    API_KEY = config['api_key']

# Cities list
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'dev.json')
with open(config_path, 'r') as reader:
    config = json.load(reader)
cities = config['cities']

# AWS setup
s3 = boto3.client("s3")
bucket_name = 'weather-data-bucket-warehouse'

# Partitioned path
today = datetime.utcnow().strftime('%Y-%m-%d')
timestamp = datetime.utcnow().strftime("%H%M")
s3_key = f"raw/date={today}/weather_{timestamp}.jsonl"

# Collect weather data
buffer = StringIO()
for city in cities:
    params = {"q": city, "appid": API_KEY, "units": "metric"}
    response = requests.get("https://api.openweathermap.org/data/2.5/weather", params=params)
    if response.status_code == 200:
        buffer.write(json.dumps(response.json()) + "\n")
    else:
        print(f"Failed for {city}: {response.status_code} - {response.text}")

# Upload directly to S3
s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue().encode("utf-8"))

print(f"✅ Uploaded to s3://{bucket_name}/{s3_key}")
