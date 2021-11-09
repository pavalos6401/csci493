#!/usr/bin/env bash

# Remove old data
rm -rfv AoT_Chicago.complete.temp2.tar.gz
rm -rfv AoT_Chicago.complete.temp2

# Fetch new data and decompress it
wget https://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/slices/AoT_Chicago.complete.temp2.tar.gz
tar -xvf AoT_Chicago.complete.temp2.tar.gz
gzip -vd --keep AoT_Chicago.complete.temp2/data.csv.gz
