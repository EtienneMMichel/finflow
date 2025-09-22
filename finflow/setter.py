from . import utils
import datetime as dt
from .utils.exceptions import MissingConfigException, MissingDataException

DEFAULT_DATA_LENGTH = 100000

def check_format_candles_price(data):
    '''
    TO-DO: add robust checks to 'content'
    '''
    market = data.get("market",None)
    if market is None:
        raise MissingConfigException("no key 'market' found in datas element (either 'spot' or 'future')")
    exchange = data.get("exchange",None)
    if exchange is None:
        raise MissingConfigException("no key 'exchange' found in datas element variable")
    symbol = data.get("symbol",None)
    if symbol is None:
        raise MissingConfigException("no key 'symbol' found in datas element variable")
    timeframe = data.get("timeframe",None)
    if timeframe is None:
        raise MissingConfigException("no key 'timeframe' found in datas element variable")
    timeframe = data.get("timeframe",None)
    if timeframe is None:
        raise MissingConfigException("no key 'timeframe' found in datas element variable")
    data_content = data.get("data",None)
    if not isinstance(data_content, list):
        raise MissingConfigException("no key 'data' found in datas element variable or not a list")
    
    d = data_content[0]
    if not "timestamp" in list(d.keys()) or not isinstance(d["timestamp"], int) or len(str(d["timestamp"])) != 13: raise MissingDataException("no 'timestamp' or incorrect format")
    if not "open" in list(d.keys()) or not isinstance(d["open"], str): raise MissingDataException("no 'open' or incorrect format")
    if not "high" in list(d.keys()) or not isinstance(d["high"], str): raise MissingDataException("no 'high' or incorrect format")
    if not "low" in list(d.keys()) or not isinstance(d["low"], str): raise MissingDataException("no 'low' or incorrect format")
    if not "close" in list(d.keys()) or not isinstance(d["close"], str): raise MissingDataException("no 'close' or incorrect format")
    if not "volume" in list(d.keys()) or not isinstance(d["volume"], str): raise MissingDataException("no 'volume' or incorrect format")


def check_format_forecast_direction(data):
    pass

def check_format_forecast_vol(data):
    pass

def check_format_diff_fundings(data):
    """
        format : [{
            "timestamp":int
            "base_asset":str
            "diff":str

            "short_leg":str
            "short_leg_exchange":str
            "short_fr":str

            "long_leg":str
            "long_leg_exchange":str
            "long_fr":str
        }]
        """
    pass

def check_format_fundings(data):
    exchange = data.get("exchange", None)
    if not isinstance(exchange, str):
        raise MissingDataException("no 'exchange' key or incorrect format")
    
    records = data.get("data", None)
    if not isinstance(records, list):
        raise MissingDataException("no 'datas' key or incorrect format (must be a list of dict)")
    if len(records) > 0:
        if not isinstance(records[0].get("timestamp", None), int):
            raise MissingDataException("no 'timestamp' key or incorrect format (must be an int)")
        if not isinstance(records[0].get("quote_asset", None), str):
            raise MissingDataException("no 'quote_asset' key or incorrect format (must be an str)")
        if not isinstance(records[0].get("base_asset", None), str):
            raise MissingDataException("no 'base_asset' key or incorrect format (must be an str)")
        if not isinstance(records[0].get("funding_rate", None), str):
            raise MissingDataException("no 'funding_rate' key or incorrect format (must be an str)")
        if not isinstance(records[0].get("mark_price", None), str):
            raise MissingDataException("no 'mark_price' key or incorrect format (must be an str)")

async def set_data(content):
    data_length = content.get("data_length", DEFAULT_DATA_LENGTH)
    if content.get("type",None) == "candles_price":
        for data in content.get("datas", []):
            check_format_candles_price(data)
            await utils.set_candles_price(data=data["data"], market=data["market"], exchange=data["exchange"], symbol=data["symbol"], timeframe=data["timeframe"], data_length=data_length)
    elif content.get("type",None) == "forecast-direction":
        data = content.get("datas", None)
        if not isinstance(data, list):
            raise MissingDataException("Failed to get 'datas' key or 'datas' is not a list")
        check_format_forecast_direction(data)
        await utils.set_forecast_direction(data, data_length=data_length)

    elif content.get("type",None) == "forecast-vol":
        data = content.get("datas", None)
        if not isinstance(data, list):
            raise MissingDataException("Failed to get 'datas' key or 'datas' is not a list")
        check_format_forecast_vol(data)
        await utils.set_volatility_forecast(data, data_length=data_length)

    
    elif content.get("type",None) == "fundings":
        data = content.get("datas", None)
        if not isinstance(data, dict):
            raise MissingDataException("Failed to get 'datas' key or 'datas' is not a dict")
        check_format_fundings(data)
        await utils.set_fundings(data, data_length=data_length)

    elif content.get("type",None) == "diff_fundings":
        data = content.get("datas", None)
        if not isinstance(data, list):
            raise MissingDataException("Failed to get 'datas' key or 'datas' is not a list")
        check_format_diff_fundings(data)
        await utils.set_diff_fundings(data, data_length=data_length)