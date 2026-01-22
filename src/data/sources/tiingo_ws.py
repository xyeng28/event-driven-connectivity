import asyncio
import websockets
import json
import ssl
import certifi
from dateutil import parser
from datetime import datetime as dtt
from websocket import create_connection

from constants import NY_TZ, EVENT_TYPE_TRADE, EVENT_TYPE_QUOTE, EVENT_TYPE_REF_PX, ASSET_TYPE_CRYPTO, ASSET_TYPE_FX, ASSET_TYPE_STK
from src.utils import convert_dt_to_tz

import src.data.data_config as data_cfg
from src.core.queue_manager import trade_queue, quote_queue, ref_px_queue

def get_top_book_trade_event_payload(tickers:list, threshold_level:int=6):
    '''
    A "thresholdLevel" of 0 means you will get ALL Top-of-Book AND Last Trade updates.
    A "thresholdLevel" of 5 means you will get all Last Trade updates and only Quote updates that are deemed major updates by our system.
    tickers: ['spy', 'uso']. If you pass "*" as a ticker, it will mean you will get data for ALL tickers.
    :return:
    '''
    tickers = [ticker.lower() for ticker in tickers]
    subscribe_payload = {
        'eventName': 'subscribe',
        'authorization': data_cfg.TIINGO_WS_KEY,
        'eventData': {
            'thresholdLevel': threshold_level,
            'tickers': tickers
        }
    }
    print(subscribe_payload)
    return subscribe_payload

async def tiingo_ws_request(subscribe_payload:dict, ws_url:str):
    try:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with websockets.connect(ws_url, ssl=ssl_context) as ws:
            await ws.send(json.dumps(subscribe_payload))
            while True:
                msg = await ws.recv()
                msg_json = json.loads(msg)
                print(f'msg_json:{msg_json}')
                if msg_json and msg_json.get('messageType') not in ('I', 'H'):
                    data = msg_json.get('data')
                    yield data

        # ws = create_connection(data_cfg.TIINGO_WS_BASE_URL)
        # subscribe = get_top_book_trade_event_payload(tickers)
        # ws.send(json.dumps(subscribe))
        # while True:
        #     print(ws.recv())

    except websockets.ConnectionClosed as e:
        print(f'Connection closed due to {e}. \nReconnecting...')
        await asyncio.sleep(10)

async def iex_stocks_feed(tickers:dict, threshold_level:int=6):
    subscribe_payload = get_top_book_trade_event_payload(tickers['STK']+tickers['ETF'], threshold_level=threshold_level)
    async for raw_data in tiingo_ws_request(subscribe_payload, data_cfg.TIINGO_WS_IEX_URL):
        print(raw_data)
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
        print(normalised_data)
        await ref_px_queue.put({'market_feed':normalised_data})

async def fx_feed(tickers:dict, threshold_level:int=5):
    """
    A "thresholdLevel" of 5 means you will get ALL Top-of-Book updates.
    :param tickers:
    :param threshold_level:
    :return:
    """
    subscribe_payload = get_top_book_trade_event_payload(tickers['FX'], threshold_level=threshold_level)
    async for raw_data in tiingo_ws_request(subscribe_payload, data_cfg.TIINGO_WS_FX_URL):
        print(raw_data)
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
        print(normalised_data)
        await quote_queue.put({'market_feed':normalised_data})

async def crypto_feed(tickers:dict, threshold_level:int=5):
    """
    A higher "thresholdLevel" means you will get less updates, which could potentially be more relevant.
    A "thresholdLevel" of 2 means you will get Top-of-Book AND Last Trade updates.
    A "thresholdLevel" of 5 means you will get only Last Trade updates
    :param tickers:
    :param threshold_level:
    :return:
    """
    subscribe_payload = get_top_book_trade_event_payload(tickers['CRYPTO'], threshold_level=threshold_level)
    async for raw_data in tiingo_ws_request(subscribe_payload, data_cfg.TIINGO_WS_CRYPTO_URL):
        print(raw_data)
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
        print(normalised_data)
        await queue.put({'market_feed':normalised_data})








