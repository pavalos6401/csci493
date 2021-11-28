#!/usr/bin/env python3

"""
Normalize the given data csv file.

Pick values at given time intervals (or a closest match).

Some of the date range generation is inspired by the code shown in:
https://towardsdatascience.com/basic-time-series-manipulation-with-pandas-4432afee64ea
"""


import argparse
import datetime

import pandas as pd


def parse_args():
    """
    Parse arguments given to the script.

    Returns:
        argparse.Namespace: Namespace for the parsed arguments.
    """

    CHOICES = [
        "temp2",
        "temp1",
        "humidity",
        "pressure",
        "latest",
    ]
    DEFAULT = CHOICES[0]

    parser = argparse.ArgumentParser(
        description="normalize csv to specific time intervals"
    )
    parser.add_argument(
        "--data",
        metavar="D",
        type=str,
        required=False,
        choices=CHOICES,
        default=DEFAULT,
        help=f"data to normalize: {CHOICES}; default: {DEFAULT}",
    )
    parser.add_argument(
        "--start",
        metavar="S",
        type=str,
        required=False,
        default="2018/01/01",
        help="start date for the data",
    )
    parser.add_argument(
        "--end",
        metavar="E",
        type=str,
        required=False,
        default="2020/04/03",
        help="end date for the data",
    )
    parser.add_argument(
        "--freq",
        metavar="F",
        type=str,
        required=False,
        default="0.5H",
        help="time intervals for the data",
    )

    return parser.parse_args()


def generate_template(start, end, freq):
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
    df["data"] = None
    return df


def fill_template(vsn, temp_df, data_df):
    """
    Fills in a given dataframe with the node's data.

    Parameters:
        vsn (str): Node number to use for the data.
        temp_df (pd.DataFrame): Template dataframe.
        data_df (pd.DataFrame): Data dataframe.
    """

    vsn_df = data_df[data_df["vsn"] == vsn]
    vsn_df.reset_index(drop=True, inplace=True)

    for d in temp_df["date"]:
        try:
            approx = min(vsn_df["data.csv"], key=lambda sub: abs(sub - d))
            i = vsn_df[vsn_df["data.csv"] == approx].index[0]
            temp_df.loc[lambda df: df["date"] == d, "data"] = vsn_df.iat[i, 2]
            #  temp_df[temp_df["date"] == d]["data"] = vsn_df.iat[i, 2]
        except:
            break


def main():
    """
    Main entrypoint of the program.
    """

    args = parse_args()
    # Store the arguments used for the combine script
    with open("nodes/args.csv", "w") as f:
        f.write("data,start,end,freq\n")
        f.write(f"{args.data},{args.start},{args.end},{args.freq}")

    # List of vsns to be used
    vsns = pd.read_csv(
        "selected_nodes.csv", usecols=["vsn"], squeeze=True
    ).to_list()
    # Data csv file
    data_df = pd.read_csv(
        f"AoT_Chicago.complete.{args.data}"
        f"{f'.{datetime.date.today()}' if args.data == 'latest' else ''}"
        "/data.csv",
    )
    # Parse the dates into datetime format
    data_df["data.csv"] = pd.to_datetime(
        data_df["data.csv"], format="%Y/%m/%d %H:%M:%S"
    )

    print(f"normalizing {args.data} data...")
    for vsn in vsns:
        print(f"Node {vsn}...", end="")
        df = generate_template(args.start, args.end, args.freq)
        fill_template(vsn, df, data_df)
        df.to_csv(f"nodes/node-{vsn}.csv")
        print(" done")
    print("done")


if __name__ == "__main__":
    main()
