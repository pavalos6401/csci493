#!/usr/bin/env python3

"""Request data from the array of things archive/historical data."""


from argparse import ArgumentParser
import tarfile
from time import time
import pandas as pd
import requests
from os.path import exists


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
parser.add_argument(
    "--skip",
    action="store_true",
    required=False,
    help="skip the download and uncompressing if the folder exists",
)
args = parser.parse_args()

FOL_PATH = f"AoT_Chicago.complete.{args.data}"
URL = "http://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/"
TAR_PATH = ""
if args.data == "latest":
    URL += f"{FOL_PATH}.tar"
    TAR_PATH = f"{args.data}.tar"
else:
    URL += f"slices/{FOL_PATH}.tar.gz"
    TAR_PATH = f"{args.data}.tar.gz"

if not (exists(f"./{FOL_PATH}/data.csv.gz") and args.skip):
    print(f"url: {URL}")
    print(f"store to: {TAR_PATH}")

    print("requesting compressed file...")
    start = time()
    response = requests.get(URL, stream=True)
    print(f"status code: {response.status_code}")

    if response.status_code == 200:  # 200 means the request succeeded
        with open(TAR_PATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=2048):
                f.write(chunk)
        end = time()
        print(f"time to get compressed file: {end - start}sec")

        print("uncompressing file...")
        start = time()
        with tarfile.open(TAR_PATH) as tar:
            tar.extractall(".")
        end = time()
        print(f"time to uncompress file: {end - start}sec")

print("cleaning data...")
start = time()
reader = pd.read_csv(f"{FOL_PATH}/data.csv.gz",
                     compression="gzip", parse_dates=True, chunksize=10000)
# Dictionary of node_id to vsn
selected = pd.read_csv("selected_nodes.csv", index_col=0, squeeze=True,
                       usecols=["node_id", "vsn"]).to_dict()
lat_dict = pd.read_csv(f"AoT_Chicago.complete.{args.data}/nodes.csv",
                       usecols=["vsn", "lat"], index_col=0,
                       squeeze=True).to_dict()
lon_dict = pd.read_csv(f"AoT_Chicago.complete.{args.data}/nodes.csv",
                       usecols=["vsn", "lon"], index_col=0,
                       squeeze=True).to_dict()
first = True
for chunk_df in reader:
    chunk_df = chunk_df.drop(
        columns=["subsystem", "sensor", "parameter", "value_raw"]
    )
    result_df = chunk_df[chunk_df["node_id"].isin(selected.keys())]
    result_df["vsn"] = result_df["node_id"].map(selected)
    result_df["lat"] = result_df["vsn"].map(lat_dict)
    result_df["lon"] = result_df["vsn"].map(lon_dict)
    result_df.to_csv(f"AoT_Chicago.complete.{args.data}/data.csv",
                     mode="a", header=first)
    first = False
end = time()
print(f"time to clean data: {end - start}sec")
