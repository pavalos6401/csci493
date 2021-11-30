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
        nodes_meta (pd.DataFrame): Node metadata.
        vsn_df (pd.DataFrame): Node data.
        df (pd.DataFrame): Output DataFrame of all data.

    Returns:
        (pd.DataFrame): The output dataframe, with the current row appended.
    """

    temp = vsn_df.loc[vsn_df["date"] == d]["data"]
    row = {
        "date": d,
        "vsn": vsn,
        "temp": temp.iloc[-1],
        "lat": nodes_meta.at[nodes_meta.index[0], "lat"],
        "lon": nodes_meta.at[nodes_meta.index[0], "lon"],
    }

    return df.append(row, ignore_index=True)


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
    ).tolist()
    # Metadata for all nodes being used
    nodes_meta = pd.read_csv("selected_nodes.csv")
    vsns = nodes_meta["vsn"].tolist()

    vsn_dfs = dict()
    for vsn in vsns:
        tmp = pd.read_csv(f"nodes/node-{vsn}.csv")
        tmp["date"] = pd.to_datetime(
            tmp["date"], format="%Y-%m-%d %H:%M:%S"
        )
        vsn_dfs[vsn] = tmp


    for d in date_rng:
        print(f"Date {d}... ", end="")
        for vsn in vsns:
            df = vsn_step(d, vsn, nodes_meta[nodes_meta["vsn"] == vsn], vsn_dfs[vsn], df)
        print("Done")

    df.to_csv("aot-data.csv")


if __name__ == "__main__":
    main()
