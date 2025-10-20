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
                    res["historical_candles_price"][market][exchange][f"{symbol}--{timeframe}"] = await utils.get_historical_candles_price(market, exchange, symbol, timeframe)


    
    if isinstance(data_config.get("fundings",None), dict):
        res["candles_price"] = {}
        exchanges = data_config["fundings"].get("exchanges", [])
        symbols = data_config["fundings"].get("symbols", [])
        res = await utils.get_fundings(exchanges=exchanges, symbols=symbols)

    if isinstance(data_config.get("liquidation",None), dict):
        additional_query = ""
        symbols = data_config["liquidation"].get("symbols", [])
        if len(symbols) > 0:
            additional_query += ("" if len(additional_query) == 0 else " AND ") + f"symbol IN ({', '.join(symbols)})"
        
        exchanges = data_config["liquidation"].get("exchange", [])
        if len(exchanges) > 0:
            additional_query += ("" if len(additional_query) == 0 else " AND ") + f"exchange IN ({', '.join(exchanges)})"
        
        nb_liquidation = data_config["liquidation"].get("nb_liquidation", 0)
        if nb_liquidation > 0:
            additional_query += ("" if len(additional_query) == 0 else " AND ") + f"nb_liquidation >= {nb_liquidation}"

        liquidation = data_config["liquidation"].get("liquidation", 0)
        if liquidation > 0:
            additional_query += ("" if len(additional_query) == 0 else " AND ") + f"liquidation >= {liquidation}"

        quote_asset = data_config["liquidation"].get("quote_asset", "")
        if len(quote_asset) > 0:
            additional_query += ("" if len(additional_query) == 0 else " AND ") + f"quote_asset = '{quote_asset}'"

        base_asset = data_config["liquidation"].get("base_asset", "")
        if len(base_asset) > 0:
            additional_query += ("" if len(additional_query) == 0 else " AND ") + f"base_asset = '{base_asset}'"
        
        if len(additional_query) > 0: additional_query = "WHERE " + additional_query
        
        res = await utils.get_liquidation(additional_query=additional_query)

    return res