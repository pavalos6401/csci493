#!/usr/bin/env python3

"""Request data from the array of things archive/historical data."""

from argparse import ArgumentParser
import tarfile
from time import time
import requests

DATA_CHOICES = [
    "latest",
    "temp1",
    "temp2",
    "humidity",
    "pressure",
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


URL = (
    "http://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/"
    f"slices/AoT_Chicago.complete.{args.data}.tar.gz"
)
TARGET_PATH = f"{args.data}.tar.gz"

print()
print(URL)

start = time()
response = requests.get(URL, stream=True)
print(response)

if response.status_code == 200:  # 200 means the request succeeded
    # Write the tar (compressed file)
    with open(TARGET_PATH, "wb") as f:
        f.write(response.raw.read())

    end = time()
    print(f"Time to get tar.gz: {end - start}sec")

    start = time()
    # Uncompress tar file
    with tarfile.open(TARGET_PATH) as tar:
        tar.extractall(".")

    end = time()
    print(f"Time to uncompress tar.gz: {end - start}sec")
