import pandas as pd
from ..connections.database import Database as ConnectionObject 

def set_forecast_direction(data):
    conn = ConnectionObject()
    data_length = data.get("data_length", None)
    conn.set_data(data, table_name=f"forecast_direction", data_length=data_length)