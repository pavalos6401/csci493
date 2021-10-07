import argparse
import requests
import json
import pandas as pd
from time import time


start_total: float = time()
# Command-line arguments for the script
parser = argparse.ArgumentParser(description="Retrieve Array of Things data.")
parser.add_argument(
    "--start",
    metavar="S",
    type=str,
    required=False,
    default="-1h",
    help="start time for data retrieval filter",
)
parser.add_argument(
    "--name",
    metavar="D",
    type=str,
    required=False,
    default="env.temperature",
    help="name of data to retrieve, e.g. env.temperature",
)
args = parser.parse_args()

# URL to retrieve the Array of Things data
url: str = "https://data.sagecontinuum.org/api/v1/query"

# Headers, to retrieve the json data
headers: dict[str, str] = {
    "Content-Type": "application/json",
}

# Filters for the data
data: dict[str, str] = {
    "start": args.start,
    "filter": {
        "name": args.name
    }
}

# Retrieve the data
print(f"Retrieving {args.name} data using {args.start} as start")
start: float = time()
print()
response: requests.Response = requests.post(
    url,
    headers=headers,
    data=json.dumps(data),
)
end: float = time()
print(f"Time to retrieve data: {end - start}sec")

# List of all the datasets as a dictionaries
data_list: list[dict[str, str]] = []

# Convert the text to a list of json strings
split_text: list[str] = response.text.split("\n")
# Remove the last element, which is a trialing whitespace
del split_text[-1]

# Iterate through and clean the data,
# Separate "meta" key (dictionary) into different keys
# Remove unncecessary "name" key with the value "env.temperature"
# Rename "value" key to "temp" for clarity
start: float = time()
for i in split_text:
    j = json.loads(i.replace('\\"', '"'))
    for key in ["host", "job", "node", "plugin", "sensor", "task", "vsn"]:
        j[key] = j["meta"][key]
    j.pop("meta", None)
    j[args.name] = j["value"]
    j.pop("name", None)
    j.pop("value", None)
    data_list.append(j)
end: float = time()
print(f"Time to clean data: {end - start}sec")

# Convert the above data to a dataframe (table)
df: pd.DataFrame = pd.DataFrame(data_list)
# Dump the table into a csv file
df.to_csv("data.csv", index=False)
end_total: float = time()
print(f"Total time: {end_total - start_total}sec")
