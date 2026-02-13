import pandas as pd
import pytz
from datetime import datetime as dtt
from src.constants import NY_TZ
from src.logger import get_logger

logger = get_logger(__name__)

def load_tickers(fp='src/data/tickers.csv') -> dict:
    """
    Loads tickers from csv file and groups them by asset type
    """
    logger.info(f'Loading tickers from {fp}')
    df = pd.read_csv(fp)[['symbol','asset_type']]
    tickers = df.groupby('asset_type')['symbol'].unique().apply(list).to_dict()
    logger.info(f'tickers:{tickers}')
    return tickers

def convert_dt_to_tz(timestamp: dtt, tz:pytz.BaseTzInfo|None=None) -> dtt:
    """
    Convert a datetime object to the specified timezone, defaults to New York timezone (NY_TZ)
    """
    logger.debug(f'Converting timestamp to tz')
    if tz is None:
        tz = NY_TZ
    if timestamp.tzinfo is None:
        timestamp = pytz.UTC.localize(timestamp)
    return timestamp.astimezone(tz)
