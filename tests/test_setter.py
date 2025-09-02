from finflow.setter import set_data
import pandas as pd
import asyncio
import random as rd
import datetime as dt

async def test_set_candles_price():
    data = []
    for _ in range(3):
        r = rd.randint(1,10)
        d = dt.datetime.now() + dt.timedelta(days=r)
        data.append({"datetime":d,"days":r})
    content = {
        "type": "candles_price",
        "data_length": 5,
        "datas": [
            {
                "market":"spot",
                "exchange": "binance",
                "symbol": "BTC_USDT-test",
                "timeframe": "1m",
                "data": data
            }
        ]
    }
    await set_data(content)

if __name__ == "__main__":
    asyncio.run(test_set_candles_price())
