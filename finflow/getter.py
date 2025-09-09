from . import utils
from .utils.exceptions import MissingConfigException, ConfigException

async def get_data(config):
    data_config = config
    res = {}

    if isinstance(data_config.get("candles_price",None), dict):
        res["candles_price"] = {}
        for market, market_config in data_config["candles_price"].items():
            res["candles_price"][market] = {}
            for exchange, exchange_config in market_config.items():
                res["candles_price"][market][exchange] = {}
                for symbol_timeframe in exchange_config:
                    if not isinstance(symbol_timeframe, dict):
                        raise ConfigException("symbol_timeframe should be a dict with keys ('symbol', 'timeframe')")
                    symbol, timeframe = symbol_timeframe.get("symbol", None), symbol_timeframe.get("timeframe", None)
                    res["candles_price"][market][exchange][f"{symbol}--{timeframe}"] = await utils.get_candles_price(market, exchange, symbol, timeframe)


    if isinstance(data_config.get("historical_candles_price",None), dict):
        res["historical_candles_price"] = {}
        for market, market_config in data_config["historical_candles_price"].items():
            res["historical_candles_price"][market] = {}
            for exchange, exchange_config in market_config.items():
                res["historical_candles_price"][market][exchange] = {}
                for symbol_timeframe in exchange_config:
                    if not isinstance(symbol_timeframe, dict):
                        raise ConfigException("symbol_timeframe should be a dict with keys ('symbol', 'timeframe')")
                    symbol, timeframe = symbol_timeframe.get("symbol", None), symbol_timeframe.get("timeframe", None)
                    res["historical_candles_price"][market][exchange][f"{symbol}--{timeframe}"] = utils.get_historical_candles_price(market, exchange, symbol, timeframe)


    
    if isinstance(data_config.get("fundings",None), dict):
        res["candles_price"] = {}
        exchanges = data_config["fundings"].get("exchanges", [])
        symbols = data_config["fundings"].get("symbols", [])
        res = await utils.get_fundings(exchanges=exchanges, symbols=symbols)

    return res