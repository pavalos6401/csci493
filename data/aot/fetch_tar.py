#!/usr/bin/env python3

"""Request data from the array of things archive/historical data."""


from argparse import ArgumentParser
import tarfile
from time import time
import pandas as pd
import requests


DATA_CHOICES = [
    "temp2",
    "temp1",
    "humidity",
    "pressure",
    "latest",
]

parser = ArgumentParser(
    description="retrieve historical data from the array of things archive",
)
parser.add_argument(
    "--data",
    metavar="D",
    type=str,
    required=False,
    choices=DATA_CHOICES,
    default=DATA_CHOICES[0],
    help=f"data to retrieve, options: {DATA_CHOICES}, default: {DATA_CHOICES[0]}",
)
args = parser.parse_args()

URL = "http://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/"
TARGET_PATH = ""
if args.data == "latest":
    URL += f"AoT_Chicago.complete.{args.data}.tar"
    TARGET_PATH = f"{args.data}.tar"
else:
    URL += f"slices/AoT_Chicago.complete.{args.data}.tar.gz"
    TARGET_PATH = f"{args.data}.tar.gz"

print(f"url: {URL}")
print(f"store to: {TARGET_PATH}")

start = time()
response = requests.get(URL, stream=True)
print(f"status code: {response.status_code}")

if response.status_code == 200:  # 200 means the request succeeded
    # Write the tar (compressed file)
    with open(TARGET_PATH, "wb") as f:
        f.write(response.raw.read())
    end = time()
    print(f"Time to get compressed file: {end - start}sec")

    start = time()
    with tarfile.open(TARGET_PATH) as tar:
        tar.extractall(".")
    end = time()
    print(f"Time to uncompress file: {end - start}sec")

    start = time()
    reader = pd.read_csv(f"AoT_Chicago.complete.{args.data}/data.csv.gz",
                         compression="gzip", parse_dates=True, chunksize=10000)
    # Dictionary of node_id to vsn
    selected = pd.read_csv("selected_nodes.csv",
                           index_col=0, squeeze=True).to_dict()
    for chunk_df in reader:
        chunk_df = chunk_df.drop(columns=["subsystem", "sensor", "parameter"])
        result_df = chunk_df[chunk_df["node_id"].isin(selected.keys())]
        result_df["vsn"] = result_df["node_id"].map(selected)
        result_df.to_csv(f"AoT_Chicago.complete.{args.data}/data.csv",
                         mode="a")
    end = time()
    print(f"Time to clean data: {end - start}sec")
