from argparse import ArgumentParser
from requests import Response, post
from time import time
from pandas import DataFrame
import json


start_total = time()
# Command-line arguments for the script
parser = ArgumentParser(
    description="Retrieve Array of Things data.",
)
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
url = "https://data.sagecontinuum.org/api/v1/query"

# Headers, to retrieve the json data
headers = {
    "Content-Type": "application/json",
}

# Filters for the data
data = {
    "start": args.start,
    "filter": {
        "name": args.name
    }
}

# Retrieve the data
print(f"Retrieving {args.name} data from {args.start}")
start = time()
print()
response = post(
    url,
    headers=headers,
    data=json.dumps(data),
)
end = time()
print(f"Time to retrieve data: {end - start} sec")

# List of all the datasets as a dictionaries
data_list = []

# Convert the text to a list of json strings
split_text = response.text.split("\n")
# Remove the last element, which is a trailing whitespace
del split_text[-1]

# Save the first one as a sample of the data to be used
#  with open("sample.json", "w") as f:
#      json.dump(
#          json.loads(split_text[0].replace('\\"', '"')),
#          f,
#          indent=4,
#          sort_keys=True,
#      )

# Iterate through and clean the data,
# Separate "meta" key (dictionary) into different keys
# Remove unncecessary "name" key with the value "env.temperature"
# Rename "value" key to "temp" for clarity
start = time()
for i in split_text:
    j = json.loads(i.replace('\\"', '"'))
    for key in j["meta"]:
        j[key] = j["meta"][key]
    j.pop("meta", None)
    j[args.name] = j["value"]
    j.pop("name", None)
    j.pop("value", None)
    data_list.append(j)
end = time()
print(f"Time to clean data: {end - start} sec")

# TODO The code below could be done simultaneously while cleaning the data
# Convert the above data to a dataframe (table)
df: DataFrame = DataFrame(data_list)
# Dump the table into a csv file
df.to_csv("data.csv", index=False)
end_total = time()
print(f"Total time: {end_total - start_total} sec")
