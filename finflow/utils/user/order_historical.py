from ..connections.database import Database as ConnectionObject 

async def set_orders_historical(data):
    conn = ConnectionObject()
    conn.upload_data(data, "orders_historical", conflict_cols=["order_id"])