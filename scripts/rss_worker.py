# scripts/rss_worker.py

import os
import json
import base64
import toml
import xmltodict
import requests

RSS_URL = "https://rss.accuweather.com/rss/liveweather_rss.asp?metric=1&locCode=ASI%7CIN%7CKA%7CBENGALURU"
TOPIC = "weather-updates"
SUB = "weather-sub"
BUCKET = "weather-xml-json-storage"

timestamp = os.popen("date +%Y%m%d_%H%M%S").read().strip()
json_file = f"rss_{timestamp}.json"
toml_file = f"bengaluru_weather_{timestamp}.toml"

# Step 1: Download XML
xml = requests.get(RSS_URL).content

# Step 2: Convert XML to JSON
json_data = xmltodict.parse(xml)

with open(json_file, "w") as f:
    json.dump(json_data, f, indent=2)

# Step 3: Publish JSON to Pub/Sub
b64 = base64.b64encode(open(json_file, "rb").read()).decode()
os.system(f'gcloud pubsub topics publish {TOPIC} --message="{b64}"')

# Step 4: Pull from subscription
out = os.popen(f'gcloud pubsub subscriptions pull {SUB} --limit=1 --auto-ack --format="value(message.data)"').read()
decoded = base64.b64decode(out.strip())

with open(json_file, "wb") as f:
    f.write(decoded)

# Step 5: Convert JSON to TOML
with open(json_file) as jf:
    data = json.load(jf)
with open(toml_file, "w") as tf:
    toml.dump(data, tf)

# Step 6: Upload TOML to GCS
os.system(f'gsutil cp {toml_file} gs://{BUCKET}/{toml_file}')
