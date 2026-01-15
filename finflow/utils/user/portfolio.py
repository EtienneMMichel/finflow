from ..connections.database import Database as ConnectionObject 
import time

async def set_portfolio(data):
    conn = ConnectionObject()
    now = int(str(int(time.time())) + "000")
    if len(data["positions"]) > 0:
        for i in range(len(data["positions"])): data["positions"][i]["timestamp"] = now
        conn.upload_data(data["positions"], "portfolio_positions", conflict_cols=["order_id"])
    
    if len(data["balance"]) > 0:
        for i in range(len(data["balance"])): data["balance"][i]["timestamp"] = now
        conn.upload_data(data["balance"], "portfolio_balance", conflict_cols=["market", "exchange", "symbol", "timestamp"])