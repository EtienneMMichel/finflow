import pandas as pd
from ..connections.database import Database as ConnectionObject 

async def set_volatility_forecast(data, data_length=None):
    conn = ConnectionObject()
    conn.save_dataframe(pd.DataFrame(data), table_name=f"volatility_forecast", data_length=data_length)