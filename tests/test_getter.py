from finflow.getter import get_data
import pandas as pd
import asyncio

async def test_get_candles_price():
    config = {
        "candles_price":{
            "spot":{
                "binance":[
                    {
                        "symbol":"BTC_USDT-test",
                        "timeframe":"1m"
                    }
                ]
            }
        }
    }
    res = await get_data(config)
    print(res)

async def test_get_fundings():
    config = {
        "data":{
            "fundings":{
                "exchanges": [],
                "symbols": []
            }
        }
    }
    data = await get_data(config["data"])
    print(data)

if __name__ == "__main__":
    asyncio.run(test_get_fundings())
