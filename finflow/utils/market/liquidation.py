import pandas as pd
from ..connections.database import Database as ConnectionObject 

async def set_liquidation(data, data_length=None):
    conn = ConnectionObject()
    conn.upload_data(data, "liquidation", conflict_cols=["timestamp", "exchange", "symbol", "side"])


async def get_liquidation(additional_query=""):
    '''
    TO-DO: filter by symbols
    '''
    conn = ConnectionObject()
    res = conn.get_table("liquidation", additional_query=additional_query)
    return res # res.to_dict(orient="records")