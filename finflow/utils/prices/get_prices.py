from ..connections.database import Database as ConnectionObject
from binance_historical import extract_klines
import datetime as dt

async def get_candles_price(market, exchange, symbol, timeframe):
    conn = ConnectionObject()
    return conn.get_table(f"candles_price-{market}-{exchange}-{symbol}-{timeframe}")

async def get_historical_candles_price(market, exchange, symbol, timeframe):
    if exchange == "binance":
        end_date = dt.datetime.today()
        start_date = end_date - dt.timedelta(days=365*10)
        data = extract_klines([symbol], [timeframe], start_date, end_date, is_local=True)
        res = data.get(symbol,{}).get(timeframe, None)
        return res
    return None