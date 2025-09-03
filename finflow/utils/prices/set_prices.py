from ..connections.database import Database as ConnectionObject



async def set_candles_price(data, market, exchange, symbol, timeframe, data_length=None):
    conn = ConnectionObject()
    conn.set_data(data, table_name=f"candles_price-{market}-{exchange}-{symbol}-{timeframe}", data_length=data_length)