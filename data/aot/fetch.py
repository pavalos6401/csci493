import requests
import json
import pandas as pd


# URL to retrieve the Array of Things data
url: str = "https://data.sagecontinuum.org/api/v1/query"

# Headers, to retrieve the json data
headers: dict[str, str] = {
    "Content-Type": "application/json",
}

# Filters for the data
data: dict[str, str] = {
    "start": "-1h",
    "filter": {
        "name": "env.temperature"
    }
}

# Retrieve the data
response: requests.Response = requests.post(
    url,
    headers=headers,
    data=json.dumps(data),
)

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
for i in split_text:
    j = json.loads(i.replace('\\"', '"'))
    for key in ["host", "job", "node", "plugin", "sensor", "task", "vsn"]:
        j[key] = j["meta"][key]
    j.pop("meta", None)
    j["temp"] = j["value"]
    j.pop("name", None)
    j.pop("value", None)
    data_list.append(j)

# Convert the above data to a dataframe (table)
df: pd.DataFrame = pd.DataFrame(data_list)
# Dump the table into a csv file
df.to_csv("data.csv", index=False)
