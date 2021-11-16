#!/usr/bin/env python3

"""
Normalize the given data csv file.

Pick values at given time intervals (or a closest match).

Some of the date range generation is inspired by the code shown in:
https://towardsdatascience.com/basic-time-series-manipulation-with-pandas-4432afee64ea
"""


from argparse import ArgumentParser
from datetime import datetime

import numpy as np
import pandas as pd


def parse_args():
    """
    Parse arguments given to the script.

    Returns:
        argparse.Namespace: Namespace for the parsed arguments.
    """

    DATA_CHOICES = [
        "temp2",
        "temp1",
        "humidity",
        "pressure",
        "latest",
    ]
    DATA_DEFAULT = DATA_CHOICES[0]

    parser = ArgumentParser(
        description="normalize csv to specific time intervals"
    )
    parser.add_argument(
        "--data",
        metavar="D",
        type=str,
        required=False,
        choices=DATA_CHOICES,
        default=DATA_DEFAULT,
        help=f"data to normalize: {DATA_CHOICES}, default: {DATA_DEFAULT}",
    )

    return parser.parse_args()


def generate_template(start="2018/01/01", end="2020/04/03", freq="0.5H"):
    """
    Generate a template dataframe for a single node's data.

    Parameters:
        start (str): Start date for the data, inclusive.
        end (str): End date for the data, inclusive.
        freq (str): Time intervals.

    Returns:
        pd.DataFrame: Template for the dataframe of any node's temperature data
        with given time intervals.
    """

    date_rng = pd.date_range(start=start, end=end, freq=freq)
    df = pd.DataFrame(date_rng, columns=["date"])
    df["data"] = np.random.randint(-5, 100, size=(len(date_rng)))
    return df


def main():
    """
    Main entrypoint of the program.
    """

    args = parse_args()

    vsns = pd.read_csv(
        "selected_nodes.csv", usecols=["vsn"], squeeze=True
    ).to_list()

    print(f"normalizing {args.data} data...")
    for vsn in vsns:
        df = generate_template()
        # TODO: Fill in the blanks
        #  print(df.head())
        #  print(df.tail())
        # TODO: Save to node/{vsn}.csv
        exit()


if __name__ == "__main__":
    main()
