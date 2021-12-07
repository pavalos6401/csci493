#/usr/bin/env python3

"""
Combine node data into a single csv file.

This csv file will have the following columns:
    node_id,vsn,lat,lon,addr,temp[i],[...]
"""


import pandas as pd


def main():
    """
    Main entrypoint of the program.
    """

    # Output DataFrame
    df = pd.read_csv("selected_nodes.csv")

    # Arguments used when normalizing the data (when normalize.py was run)
    args_df = pd.read_csv("nodes/args.csv")
    # Date range used in the normalize script
    date_rng = pd.date_range(
        start=args_df.iat[0, 1], end=args_df.iat[0, 2], freq=args_df.iat[0, 3]
    ).tolist()
    # Metadata for all nodes being used
    nodes_meta = pd.read_csv("selected_nodes.csv")
    # List of node numbers
    vsns = nodes_meta["vsn"].tolist()

    # Mapping of vsn to its corresponding data file
    vsn_dfs = dict()
    for vsn in vsns:
        tmp = pd.read_csv(f"nodes/node-{vsn}.csv")
        tmp["date"] = pd.to_datetime(
            tmp["date"], format="%Y-%m-%d %H:%M:%S"
        )
        vsn_dfs[vsn] = tmp

    # For each date in the range, add the temperature column
    i = 0  # Current enumerated temperature in range
    for d in date_rng:
        print(f"Date {d} ... ", end="")

        df[f"temp{i}"] = "NaN"
        for vsn in vsns:
            # Metadata of node
            node_meta = nodes_meta[nodes_meta["vsn"] == vsn]
            # Data of node
            vsn_df = vsn_dfs[vsn]
            # Temperature found, as a pd.Series
            temp = vsn_df.loc[vsn_df["date"] == d]["data"]
            # Add the current node's data to the column
            df.at[df["vsn"] == vsn, f"temp{i}"] = f"{d}: {temp.iloc[-1]}"

        i += 1

        print("Done")

    # Save data to a csv file
    df.to_csv("aot-data.csv")


if __name__ == "__main__":
    main()
