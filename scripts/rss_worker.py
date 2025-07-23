import requests
import xmltodict
import json
import toml
import os
from datetime import datetime
import subprocess

# 1. Fetch RSS XML
rss_url = "https://rss.accuweather.com/rss/liveweather_rss.asp?metric=1&locCode=ASI%7CIN%7CKA%7CBENGALURU"
response = requests.get(rss_url)
xml = response.text

# 2. Try parsing XML safely
try:
    json_data = xmltodict.parse(xml)
except Exception as e:
    print("Failed to parse XML:", e)
    print("Raw XML:")
    print(xml)
    exit(1)

# 3. Convert to JSON and TOML
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
json_file = f"/tmp/rss_weather_{timestamp}.json"
toml_file = f"/tmp/rss_weather_{timestamp}.toml"

with open(json_file, "w") as f_json:
    json.dump(json_data, f_json, indent=2)

with open(toml_file, "w") as f_toml:
    toml.dump(json_data, f_toml)

# 4. Publish JSON to Pub/Sub
subprocess.run([
    "gcloud", "pubsub", "topics", "publish", "weather-topic",
    "--message", json.dumps(json_data)
], check=True)

# 5. Upload TOML to GCS
subprocess.run([
    "gcloud", "storage", "cp", toml_file, f"gs://weather-xml-json-toml-storage
/rss_data/"
], check=True)
