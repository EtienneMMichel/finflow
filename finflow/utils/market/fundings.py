import pandas as pd
from ..connections.database import Database as ConnectionObject 

async def set_fundings(data, data_length=None):
    conn = ConnectionObject()
    # 
    exchange = data["exchange"]
    records = data["data"]
    for i in range(len(records)):records[i]["exchange"] = exchange
    conn.upsert_records(records, table_name=f"fundings", conflict_cols=["timestamp", "exchange"])