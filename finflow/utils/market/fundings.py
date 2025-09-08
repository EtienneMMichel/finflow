import pandas as pd
from ..connections.database import Database as ConnectionObject 

async def set_fundings(data, data_length=None):
    conn = ConnectionObject()
    # conn.save_dataframe(pd.DataFrame(data), table_name=f"volatility_prediction", data_length=data_length)
    exchange = data["exchange"]
    records = data["data"]
    df = pd.DataFrame(records)
    df["exchange"] = [exchange for _ in range(df.shape[0])]
    conn.save_dataframe(df, "fundings")