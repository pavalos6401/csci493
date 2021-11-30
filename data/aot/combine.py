#/usr/bin/env python3

"""
Combine node data into a single csv file.

This csv file will have the following columns:
    node_id,vsn,lat,lon,temp[i],[...]
"""


import pandas as pd


def main():
    """
    Main entrypoint of the program.
    """

    # Output DataFrame
    df = pd.read_csv("selected_nodes.csv")

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

    i = 0
    for d in date_rng:
        print(f"Date {d} ... ", end="")

        df[f"temp{i}"] = "NaN"
        for vsn in vsns:
            node_meta = nodes_meta[nodes_meta["vsn"] == vsn]
            vsn_df = vsn_dfs[vsn]
            temp = vsn_df.loc[vsn_df["date"] == d]["data"]
            df.at[df["vsn"] == vsn, f"temp{i}"] = f"{d}: {temp.iloc[-1]}"
        i += 1

        print("Done")

    df.to_csv("aot-data.csv")


if __name__ == "__main__":
    main()
