import pandas as pd
import networkx as nx
import uuid


def generate_uuid():
    return str(uuid.uuid4())


def get_group(df: pd.DataFrame, user_col: str, deposit_col: str):
    # Create a graph from the merged dataframe
    G = nx.from_pandas_edgelist(df, f'{user_col}', f'{deposit_col}')

    # Find the connected components in the graph
    connected_components = nx.connected_components(G)

    # Convert the connected components to a list
    groups = [list(component) for component in connected_components]

    # Map the groups back to the original items in DataFrame A and B
    grouped_items = []
    for group in groups:
        items = list(group)
        edges = list(G.subgraph(group).edges())
        grouped_items.append([items, edges])
    # Convert the result to a DataFrame
    result = pd.DataFrame(grouped_items, columns=['grouping', 'Edges'])
    return result


def get_user_depo(df: pd.DataFrame, group_col: str, specific_list: list):
    df_groupp = df.copy()
    df_groupp['_id'] = df_groupp.apply(lambda _: generate_uuid(), axis=1)
    df_groupp = df_groupp.rename(columns={"index":"_id"})
    df_ex = df_groupp[["_id","grouping"]].explode(f'{group_col}')
    df_edges = df_groupp[["_id", "Edges"]]
    # df_ex =df_ex.rename(columns={"index":"_id"})

    df_ex_filter = df_ex[df_ex[f'{group_col}'].isin(specific_list)]
    df_ex_filter = df_ex_filter.rename(columns={f'{group_col}': "user_wallets"})
    df_ex_filter_not =  df_ex[~df_ex[f'{group_col}'].isin(specific_list)]
    df_ex_filter_not = df_ex_filter_not.rename(columns={f'{group_col}': "deposit_wallets"})

    df_filter = df_ex_filter.groupby("_id").agg({"user_wallets":list}).reset_index()
    df_filter_not = df_ex_filter_not.groupby("_id").agg({"deposit_wallets":list}).reset_index()
    df_split = df_filter.merge(df_filter_not, on= "_id", how="inner")
    df_split_edges = df_split.merge(df_edges, on= "_id", how="inner")
    return df_split_edges


def get_group_full(df: pd.DataFrame):
    df_group = get_group(df, "from_address","to_address")
    specialListt = df.from_address.unique().tolist()

    df_group_full = get_user_depo(df_group, "grouping", specialListt)
    df_group_full["num_user"] = df_group_full["user_wallets"].apply(len)

    df_group_full["num_depo"] = df_group_full["deposit_wallets"].apply(len)   
    return df_group_full


def get_groupp(df_filter: pd.DataFrame, Chain: str):
    overlap_group = get_group_full(df_filter)
    overlap_group["Chain"] = f'{Chain}'
    overlap_group['_id'] = overlap_group.apply(lambda _: generate_uuid(), axis=1)
