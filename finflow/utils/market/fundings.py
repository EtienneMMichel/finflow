import pandas as pd
from ..connections.database import Database as ConnectionObject 

async def set_fundings(data, data_length=None):
    conn = ConnectionObject()
    exchange = data["exchange"]
    records = data["data"]
    for i in range(len(records)):records[i]["exchange"] = exchange
    conn.upload_data(records, "fundings", conflict_cols=["timestamp", "exchange", "base_asset", "quote_asset"])


async def get_fundings(exchanges=[], symbols=[]):
    '''
    TO-DO: filter by exchanges and symbols
    '''
    conn = ConnectionObject()
    additional_query = ""
    res = conn.get_table("fundings", additional_query=additional_query)
    return res # res.to_dict(orient="records")


# -------------------------------------------------------------------------------------------------------------------------------------------------


async def set_diff_fundings(data, data_length=None):
    conn = ConnectionObject()
    conn.upload_data(data, "diff_fundings", conflict_cols=[])

async def get_diff_fundings(symbols=[]):
    '''
    TO-DO: filter by symbols
    '''
    conn = ConnectionObject()
    additional_query = ""
    res = conn.get_table("diff_fundings", additional_query=additional_query)
    return res # res.to_dict(orient="records")