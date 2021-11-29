#/usr/bin/env python3

"""
Combine node data into a single csv file.

This csv file will have the following columns:
    date,vsn,temp,lat,lon
"""


import pandas as pd


def vsn_step(d, vsn, nodes_meta, vsn_df, df):
    """
    Store the vsn data into the given output dataframe.

    Params:
        d (datetime.datetime): Date.
        vsn (str): Node number.
        df (pd.DataFrame): DataFrame of all data.
    """

    row = pd.Series(
        [
            d,
            vsn,
            vsn_df.loc[df["date"] == d]["data"],
            nodes_meta.loc[nodes_meta["vsn"] == vsn]["lat"],
            nodes_meta.loc[nodes_meta["vsn"] == vsn]["lon"],
        ],
        index=df.columns
    )

    df.append(row, ignore_index=True, inplace=True)


def main():
    """
    Main entrypoint of the program.
    """

    # Output DataFrame
    df = pd.DataFrame(columns=["date", "vsn", "temp", "lat", "lon"])

    # Arguments used when normalizing the data
    args_df = pd.read_csv("nodes/args.csv")
    # Date range used in the normalize script
    date_rng = pd.date_range(
        start=args_df.iat[0, 1], end=args_df.iat[0, 2], freq=args_df.iat[0, 3]
    )
    # Metadata for all nodes being used
    nodes_meta = pd.read_csv("selected_nodes.csv")
    vsns = nodes_meta["vsn"].tolist()

    vsn_dfs = dict()
    for vsn in vsns:
        vsn_dfs[vsn] = pd.read_csv(f"nodes/vsn-{vsn}.csv")

    for d in date_rng:
        for vsn in vsns:
            vsn_step(d, vsn, nodes_meta, vsn_dfs[vsn], df)

    df.to_csv("aot-data.csv")


if __name__ == "__main__":
    main()
