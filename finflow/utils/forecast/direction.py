import pandas as pd
from ..connections.database import Database as ConnectionObject 

async def set_forecast_direction(data, data_length=None):
    conn = ConnectionObject()
    # conn.save_dataframe(pd.DataFrame(data), table_name=f"forecast_direction", data_length=data_length)
    conn.upsert_records(data, "forecast_direction", conflict_cols=["timestamp", "bot_id", "symbol", "timeframe"])