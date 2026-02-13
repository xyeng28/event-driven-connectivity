"""
Tiingo WebSocket Feed Module

Provides asynchronous functions to connect to Tiingo WebSocket feeds for
stocks, crypto, and FX market data.

Key features:
- Subscribes to market data with customisable payloads and threshold levels
- Yields live market data as async generator objects
- Simple normalisation of data from different market feeds
- Automatically reconnects if the WebSocket connection closes
"""

import asyncio
from typing import AsyncGenerator
import websockets
import json
import ssl
import certifi
from src.logger import get_logger
from dateutil import parser
from datetime import datetime as dtt
from src.constants import NY_TZ, EVENT_TYPE_TRADE, EVENT_TYPE_QUOTE, EVENT_TYPE_REF_PX, ASSET_TYPE_CRYPTO, ASSET_TYPE_FX, ASSET_TYPE_STK
from src.utils import convert_dt_to_tz
import src.data.data_config as data_cfg
from src.core.queue_manager import trade_queue, quote_queue, ref_px_queue

logger = get_logger(__name__)

def get_top_book_trade_event_payload(tickers:list, threshold_level:int=6) -> dict:
    """
    Prepare subscribe payload for Tiingo Websocket API request

    Args:
        tickers: List of ticker symbols. "*" as a ticker symbol gets data for ALL tickers. Eg. ['spy', 'uso']
        threshold_level:
            threshold_level of 0 gets all Top-of-Book AND Last Trade updates.
            threshold_level of 5 gets all Last Trade updates and only Quote updates that are deemed major updates by Tiingo.
    Returns:
        dict: Subscribe payload for Tiingo Websocket API
    """
    tickers = [ticker.lower() for ticker in tickers]
    subscribe_payload = {
        'eventName': 'subscribe',
        'authorization': data_cfg.TIINGO_WS_KEY,
        'eventData': {
            'thresholdLevel': threshold_level,
            'tickers': tickers
        }
    }
    logger.debug(f'subscribe_payload:{subscribe_payload}')
    return subscribe_payload

async def tiingo_ws_request(subscribe_payload:dict, ws_url:str) -> AsyncGenerator[list, None]:
    """
    Connect to Tiingo Websocket feed and yield market data
    Filters out Heartbeat (H) and Connection Initialisation (I) messages

    Args:
        subscribe_payload: Subscribe payload for Tiingo Websocket API request
        ws_url: Websocket URL of the Tiingo feed
    Yields:
        list: Market data
    """
    while True:
        try:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            async with websockets.connect(ws_url, ssl=ssl_context) as ws:
                await ws.send(json.dumps(subscribe_payload))
                while True:
                    msg = await ws.recv()
                    msg_json = json.loads(msg)
                    logger.debug(f'msg_json:{msg_json}')
                    if msg_json and msg_json.get('messageType') not in ('I', 'H'):
                        data = msg_json.get('data')
                        logger.info(f'data:{data}')
                        yield data

        except websockets.ConnectionClosed as e:
            logger.error(f'Connection closed due to {e}. \nReconnecting...')
            await asyncio.sleep(10)

async def iex_stocks_feed(tickers:dict, threshold_level:int=6):
    """
    Push normalised reference price data into market_feed queue
    The message is wrapped in a dictionary with the key market_feed
    so that the consolidator processes it consistently with other market feeds

    Args:
        tickers: Dict of ticker asset type and ticker symbols
        threshold_level: threshold_level of 6 gets price updates when a reference price change is detected
    """
    subscribe_payload = get_top_book_trade_event_payload(tickers['STK']+tickers['ETF'], threshold_level=threshold_level)
    async for raw_data in tiingo_ws_request(subscribe_payload, data_cfg.TIINGO_WS_IEX_URL):
        logger.debug(f'raw_data:{raw_data}')
        date_iso, ticker, ref_px = raw_data
        timestamp = parser.isoparse(date_iso)
        timestamp = convert_dt_to_tz(timestamp)
        normalised_data = {
            'asset_type': ASSET_TYPE_STK,
            'event_type': EVENT_TYPE_REF_PX,
            'symbol': ticker,
            'price': ref_px,
            'vendor': 'tiingo',
            'source': 'tiingo_iex',
            'event_time': timestamp,
            'created_at': dtt.now(NY_TZ)
        }
        logger.debug(f'normalised_data:{normalised_data}')
        await ref_px_queue.put({'market_feed':normalised_data})

async def fx_feed(tickers:dict, threshold_level:int=5):
    """
    Push normalised FX quotes data into market_feed queue
    The message is wrapped in a dictionary with the key market_feed
    so that the consolidator processes it consistently with other market feeds

    Args:
        tickers: Dict of ticker asset type and ticker symbols
        threshold_level: threshold_level of 5 gets ALL Top-of-Book updates.
    """
    subscribe_payload = get_top_book_trade_event_payload(tickers['FX'], threshold_level=threshold_level)
    async for raw_data in tiingo_ws_request(subscribe_payload, data_cfg.TIINGO_WS_FX_URL):
        logger.debug(f'raw_data:{raw_data}')
        update_msg_type, ticker, date_iso, bid_size, bid, mid, ask_size, ask = raw_data
        timestamp = parser.isoparse(date_iso)
        timestamp = convert_dt_to_tz(timestamp)
        normalised_data = {
            'asset_type': ASSET_TYPE_FX,
            'event_type': EVENT_TYPE_QUOTE,
            'symbol': ticker,
            'bid_size': bid_size,
            'ask_size': ask_size,
            'bid': bid,
            'ask': ask,
            'mid': mid,
            'event_time': timestamp,
            'vendor': 'tiingo',
            'source': 'tiingo_fx',
            'created_at': dtt.now(NY_TZ)
        }
        logger.debug(f'normalised_data:{normalised_data}')
        await quote_queue.put({'market_feed':normalised_data})

async def crypto_feed(tickers:dict, threshold_level:int=5):
    """
    Push normalised Crypto trades and quotes data into market_feed queue
    The message is wrapped in a dictionary with the key market_feed
    so that the consolidator processes it consistently with other market feeds

    Args:
        tickers: Dict of ticker asset type and ticker symbols
        threshold_level:
            A "thresholdLevel" of 2 gets Top-of-Book AND Last Trade updates.
            A "thresholdLevel" of 5 gets only Last Trade updates
    """
    subscribe_payload = get_top_book_trade_event_payload(tickers['CRYPTO'], threshold_level=threshold_level)
    async for raw_data in tiingo_ws_request(subscribe_payload, data_cfg.TIINGO_WS_CRYPTO_URL):
        logger.debug(f'raw_data:{raw_data}')
        if raw_data[0] == 'T':
            update_msg_type, ticker, date_iso, exch, last_size, last_price = raw_data
            timestamp = parser.isoparse(date_iso)
            timestamp = convert_dt_to_tz(timestamp)
            queue = trade_queue
            normalised_data = {
                'asset_type': ASSET_TYPE_CRYPTO,
                'event_type': EVENT_TYPE_TRADE,
                'symbol': ticker,
                'last_size': last_size,
                'last_price': last_price,
                'event_time': timestamp,
                'vendor': 'tiingo',
                'source': 'tiingo_crypto',
                'created_at': dtt.now(NY_TZ)
            }
        else:
            update_msg_type, ticker, date_iso, bid_size, bid, mid, ask_size, ask = raw_data
            event_type = EVENT_TYPE_QUOTE
            timestamp = parser.isoparse(date_iso)
            timestamp = convert_dt_to_tz(timestamp)
            queue = quote_queue
            normalised_data = {
                'asset_type': ASSET_TYPE_CRYPTO,
                'event_type': event_type,
                'symbol': ticker,
                'bid_size': bid_size,
                'ask_size': ask_size,
                'bid': bid,
                'ask': ask,
                'mid': mid,
                'event_time': timestamp,
                'vendor': 'tiingo',
                'source': 'tiingo_crypto',
                'created_at': dtt.now(NY_TZ)
            }
        logger.debug(f'normalised_data:{normalised_data}')
        await queue.put({'market_feed':normalised_data})

