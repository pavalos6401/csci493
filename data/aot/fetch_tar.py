#!/usr/bin/env python3

"""
Request data from the array of things archive/historical data.
"""


from argparse import ArgumentParser
from datetime import date
from os.path import exists
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
DATA_DEFAULT = DATA_CHOICES[0]

parser = ArgumentParser(
    description="retrieve historical data from the array of things archive"
)
parser.add_argument(
    "--data",
    metavar="D",
    type=str,
    required=False,
    choices=DATA_CHOICES,
    default=DATA_DEFAULT,
    help=f"data to retrieve, options: {DATA_CHOICES}, default: {DATA_DEFAULT}",
)
parser.add_argument(
    "--skip",
    action="store_true",
    required=False,
    help="skip the download and uncompressing if the folder exists",
)
args = parser.parse_args()

URL = "http://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/"
TAR_PATH = ""
FOL_PATH = f"AoT_Chicago.complete.{args.data}"
if args.data == "latest":
    URL += f"{FOL_PATH}.tar"
    TAR_PATH = f"{args.data}.tar"
    FOL_PATH += f".{date.today()}"
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
            
            import os
            
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(tar, ".")
        end = time()
        print(f"time to uncompress file: {end - start}sec")
    else:
        exit(response.status_code)

print("cleaning data...")
start = time()
reader = pd.read_csv(
    f"{FOL_PATH}/data.csv.gz", compression="gzip", parse_dates=True,
    chunksize=10000
)
# Dictionary of node_id to vsn
selected = pd.read_csv(
    "selected_nodes.csv", index_col=0, squeeze=True, usecols=["node_id", "vsn"]
).to_dict()
lat_dict = pd.read_csv(
    f"AoT_Chicago.complete.{args.data}/nodes.csv", usecols=["vsn", "lat"],
    index_col=0, squeeze=True
).to_dict()
lon_dict = pd.read_csv(
    f"AoT_Chicago.complete.{args.data}/nodes.csv", usecols=["vsn", "lon"],
    index_col=0, squeeze=True
).to_dict()
first = True
for chunk_df in reader:
    chunk_df.drop(
        columns=["subsystem", "sensor", "parameter", "value_raw"], inplace=True
    )
    result_df = chunk_df[chunk_df["node_id"].isin(selected.keys())]
    result_df["vsn"] = result_df["node_id"].map(selected)
    result_df["lat"] = result_df["vsn"].map(lat_dict)
    result_df["lon"] = result_df["vsn"].map(lon_dict)
    result_df.to_csv(
        f"{FOL_PATH}/data.csv", mode="a", index=False, header=first
    )
    first = False
end = time()
print(f"time to clean data: {end - start}sec")
