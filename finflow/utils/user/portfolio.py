from ..connections.database import Database as ConnectionObject 
import datetime as dt


async def set_portfolio(data):
    conn = ConnectionObject()
    today = dt.datetime.now()
    today = dt.datetime(year=today.year, month=today.month, day=today.day, hour=today.hour, minute=today.minute)
    now = int(str(int(today.timestamp())) + "000")
    if len(data["positions"]) > 0:
        for i in range(len(data["positions"])): data["positions"][i]["timestamp"] = now
        conn.upload_data(data["positions"], "portfolio_positions", conflict_cols=["market","exchange","symbol","margin_coin","timestamp"])
    
    if len(data["balance"]) > 0:
        for i in range(len(data["balance"])): data["balance"][i]["timestamp"] = now
        conn.upload_data(data["balance"], "portfolio_balance", conflict_cols=["market", "exchange", "symbol", "timestamp"])