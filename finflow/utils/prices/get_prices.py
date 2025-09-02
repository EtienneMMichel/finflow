from ..connections.database import Database as ConnectionObject


async def get_candles_price(market, exchange, symbol, timeframe):
    conn = ConnectionObject()
    return await conn.get_table(f"candles_price-{market}-{exchange}-{symbol}-{timeframe}")

async def get_historical_candles_price(market, exchange, symbol, timeframe):
    if exchange == "binance":
        raise NotImplementedError("Must include binance_historical library")
    return None