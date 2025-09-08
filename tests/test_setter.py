from finflow.setter import set_data
import pandas as pd
import asyncio
import random as rd
import datetime as dt

async def test_set_candles_price():
    now = int(str(int(dt.datetime.now().timestamp())) + "000")
    data = [{'timestamp': now, 'open': '110412.01000000', 'high': '110412.01000000', 'low': '110360.25000000', 'close': '110360.25000000', 'volume': '4.46775000'}]
    content = {'type': 'candles_price',
               'data_length': 10,
               'datas':[{
                   'exchange': 'binance',
                   'market': 'spot',
                   'symbol': 'BTC_USDC_tesst',
                   'timeframe': '1m',
                   'data':data
               }]
               
    }
    await set_data(content)

async def test_forecast_direction():
    content = {'type': 'forecast-direction',
               'datas': [{'timestamp': 1756824660000, 'bot_id': "test", 'symbol': 'BTC_USDC', 'timeframe': '1m', 'direction': 'down'}, {'timestamp': 1756824720000, 'bot_id': "test", 'symbol': 'BTC_USDC', 'timeframe': '1m', 'direction': 'down'}]}

    await set_data(content)
    content = {'type': 'forecast-direction',
               'datas': [{'timestamp': 1756824660000, 'bot_id': "test", 'symbol': 'BTC_USDC', 'timeframe': '1m', 'direction': 'down'}, {'timestamp': 1756824780000, 'bot_id': "test", 'symbol': 'BTC_USDC', 'timeframe': '1m', 'direction': 'down'}]}

    await set_data(content)


async def test_fundings():
    content = {'type': 'fundings', 'datas': {'exchange': 'bitget', 'data': [{'timestamp': 1757346485611, 'symbol': 'BTCUSDT', 'base_asset': 'BTC', 'quote_asset': 'USDT', 'funding_rate': '0.000049', 'mark_price': '112501'},
                                                                             {'timestamp': 1757346485611, 'symbol': 'ETHUSDT', 'base_asset': 'ETH', 'quote_asset': 'USDT', 'funding_rate': '0.0001', 'mark_price': '4360.11'}]}}

    await set_data(content)



if __name__ == "__main__":
    asyncio.run(test_fundings())
