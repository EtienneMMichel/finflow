import pandas as pd
from ..connections.database import Database as ConnectionObject 

async def set_fundings(data, data_length=None):
    conn = ConnectionObject()
    exchange = data["exchange"]
    records = data["data"]
    for i in range(len(records)):records[i]["exchange"] = exchange
    conn.upsert_records(records, "fundings", conflict_cols=["timestamp", "exchange", "base_asset", "quote_asset"])


async def get_fundings(exchanges=[], symbols=[]):
    '''
    TO-DO: filter by exchanges and symbols
    '''
    conn = ConnectionObject()
    additional_query = ""
    res = conn.get_table("fundings", additional_query=additional_query)
    return res # res.to_dict(orient="records")