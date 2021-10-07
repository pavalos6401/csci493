import requests
import json
import pandas as pd
from time import time


# Start time for the data retrieval
start_data: str = "-1h"
print(f"Retrieving data using {start_data} as start")

# URL to retrieve the Array of Things data
url: str = "https://data.sagecontinuum.org/api/v1/query"

# Headers, to retrieve the json data
headers: dict[str, str] = {
    "Content-Type": "application/json",
}

# Filters for the data
data: dict[str, str] = {
    "start": start_data,
    "filter": {
        "name": "env.temperature"
    }
}

# Retrieve the data
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
    j["temp"] = j["value"]
    j.pop("name", None)
    j.pop("value", None)
    data_list.append(j)
end: float = time()
print(f"Time to clean data: {end - start}sec")

# Convert the above data to a dataframe (table)
df: pd.DataFrame = pd.DataFrame(data_list)
# Dump the table into a csv file
df.to_csv("data.csv", index=False)
