import pandas as pd
from ..connections.database import Database as ConnectionObject 

async def set_forecast_direction(data, data_length=None):
    conn = ConnectionObject()
    conn.set_data(data, table_name=f"forecast_direction", data_length=data_length)