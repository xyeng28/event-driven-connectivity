import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch
from websockets import ConnectionClosedOK
from datetime import datetime as dtt
from dateutil import parser

import constants
from src.data.sources.tiingo_ws import get_top_book_trade_event_payload, tiingo_ws_request, iex_stocks_feed, fx_feed, \
    crypto_feed
from src.data import data_config as data_cfg

class TestTiingoWS(unittest.TestCase):
    def test_get_top_book_trade_event_payload(self):
        tickers = ['aapl', 'msft', 'spy']
        threshold = 10
        payload = get_top_book_trade_event_payload(tickers, threshold_level=threshold)
        self.assertEquals(payload['eventName'], 'subscribe')
        self.assertEquals(payload['authorization'], data_cfg.TIINGO_WS_KEY)
        self.assertEquals(payload['eventData']['thresholdLevel'], threshold)
        self.assertEquals(payload['eventData']['tickers'], tickers)

    def test_tiingo_ws_request(self):
        subscribe_payload = {
            'eventName': 'subscribe',
            'authorization': data_cfg.TIINGO_WS_KEY,
            'eventData': {
                'thresholdLevel': 10,
                'tickers': ['aapl', 'msft', 'spy']
            }
        }
        tiingo_ws_url = 'tiingo_example_123.com/ws'
        mock_msgs = [
            json.dumps({'messageType':'I'}),
            json.dumps({'service': 'iex', 'messageType': 'A', 'data': ['1990-01-01T12:37:36.425451499-05:00', 'spy', 10.7]}),
            json.dumps({'messageType':'H'}),
            json.dumps({'service': 'iex', 'messageType': 'T', 'data': ['1990-01-01T12:37:36.425451499-05:00', 'spy', 10.7]}),
        ]
        mock_ws = AsyncMock()
        mock_ws.recv = AsyncMock(side_effect=(mock_msgs + [ConnectionClosedOK(None, None)]))
        mock_ws.send = AsyncMock()

        async def run_tiingo_ws_request():
            with patch('websockets.connect') as mock_connect:
                mock_connect.return_value.__aenter__.return_value = mock_ws
                mock_connect.return_value.__aexit__.return_value = None
                raw_data = tiingo_ws_request(subscribe_payload, tiingo_ws_url)
                return [data async for data in raw_data]
        data_feeds = asyncio.run(run_tiingo_ws_request())
        self.assertEqual(data_feeds, [['1990-01-01T12:37:36.425451499-05:00', 'spy', 10.7], ['1990-01-01T12:37:36.425451499-05:00', 'spy', 10.7]])
        mock_ws.send.assert_awaited_once_with(json.dumps(subscribe_payload))
        print('test_tiingo_ws_request completed')

    def test_iex_stocks_feed(self):
        tickers = {
            'STK':['AAPL'],
            'ETF':['SPY'],
        }

        async def mock_tiingo_ws_req(*args, **kwargs):
            yield ['1990-01-22T12:37:33.544333716-05:00', 'spy', 10.735]
            yield ['1990-01-22T12:37:33.544333716-05:00', 'aapl', 10.735]
        async def run_test_iex_stocks_feed():
            queue = asyncio.Queue()
            with patch('src.data.sources.tiingo_ws.tiingo_ws_request', mock_tiingo_ws_req), patch('src.data.sources.tiingo_ws.ref_px_queue', queue):
                await iex_stocks_feed(tickers)
            feeds = []
            while not queue.empty():
                feeds.append(await queue.get())
            return feeds
        result = asyncio.run(run_test_iex_stocks_feed())
        print(result)
        feed = result[0]['market_feed']

        exp_event_time = parser.isoparse('1990-01-22T12:37:33.544333-05:00')
        exp_event_time = exp_event_time.astimezone(constants.NY_TZ)
        exp_data = {
            'asset_type': constants.ASSET_TYPE_STK,
            'event_type': constants.EVENT_TYPE_REF_PX,
            'symbol': 'spy',
            'price': 10.735,
            'vendor': 'tiingo',
            'source': 'tiingo_iex',
            'event_time': exp_event_time,
            'created_at': dtt.now(constants.NY_TZ)
        }
        self.assertEqual(feed['asset_type'], exp_data['asset_type'])
        self.assertEqual(feed['event_type'], exp_data['event_type'])
        self.assertEqual(feed['symbol'], exp_data['symbol'])
        self.assertEqual(feed['price'], exp_data['price'])
        self.assertEqual(feed['vendor'], exp_data['vendor'])
        self.assertEqual(feed['source'], exp_data['source'])
        self.assertEqual(feed['event_time'], exp_data['event_time'])
        self.assertEqual(len(result), 2)
        self.assertIsNotNone(feed['created_at'])

    def test_fx_feed(self):
        tickers = {
            'FX':['eurnok', 'audusd'],
        }

        async def mock_tiingo_ws_req(*args, **kwargs):
            yield ['Q', 'eurnok', '1990-01-22T16:35:45.725000+00:00', 100000.0, 11.58626, 11.590365, 100000.0, 11.59447]
            yield ['Q', 'audusd', '1990-02-22T16:35:45.725000+00:00', 100000.0, 11.58626, 11.590365, 100000.0, 11.59447]
        async def run_test_fx_feed():
            queue = asyncio.Queue()
            with patch('src.data.sources.tiingo_ws.tiingo_ws_request', mock_tiingo_ws_req), patch('src.data.sources.tiingo_ws.quote_queue', queue):
                await fx_feed(tickers)
            feeds = []
            while not queue.empty():
                feeds.append(await queue.get())
            return feeds
        result = asyncio.run(run_test_fx_feed())
        print(result)
        feed = result[0]['market_feed']

        exp_event_time = parser.isoparse('1990-01-22T16:35:45.725000+00:00')
        exp_event_time = exp_event_time.astimezone(constants.NY_TZ)
        exp_data = {
            'asset_type': constants.ASSET_TYPE_FX,
            'event_type': constants.EVENT_TYPE_QUOTE,
            'symbol': 'eurnok',
            'bid_size': 100000.0,
            'ask_size': 100000.0,
            'bid': 11.58626,
            'ask': 11.59447,
            'mid': 11.590365,
            'event_time': exp_event_time,
            'vendor': 'tiingo',
            'source': 'tiingo_fx',
            'created_at': dtt.now(constants.NY_TZ)
        }
        self.assertEqual(feed['asset_type'], exp_data['asset_type'])
        self.assertEqual(feed['event_type'], exp_data['event_type'])
        self.assertEqual(feed['symbol'], exp_data['symbol'])
        self.assertEqual(feed['bid_size'], exp_data['bid_size'])
        self.assertEqual(feed['ask_size'], exp_data['ask_size'])
        self.assertEqual(feed['bid'], exp_data['bid'])
        self.assertEqual(feed['ask'], exp_data['ask'])
        self.assertEqual(feed['mid'], exp_data['mid'])
        self.assertEqual(feed['event_time'], exp_data['event_time'])
        self.assertEqual(feed['vendor'], exp_data['vendor'])
        self.assertEqual(feed['source'], exp_data['source'])
        self.assertEqual(len(result), 2)
        self.assertIsNotNone(feed['created_at'])

    def test_crypto_feed(self):
        tickers = {
            'CRYPTO':['BTCETH', 'EURUSD', 'EVXBTC'],
        }

        async def mock_tiingo_ws_req(*args, **kwargs):
            yield ['Q', 'btceth', '1990-02-22T16:35:45.725000+00:00', 100000.0, 11.58626, 11.590365, 100000.0, 11.59447]
            yield ['T', 'eurusd', '1990-01-22T17:37:32.177890+00:00', 'kraken', 49.1372016, 1.174045259378473]
        async def run_test_crypto_feed():
            trade_queue = asyncio.Queue()
            quote_queue = asyncio.Queue()
            with (patch('src.data.sources.tiingo_ws.tiingo_ws_request', mock_tiingo_ws_req), patch('src.data.sources.tiingo_ws.trade_queue', trade_queue),
                  patch('src.data.sources.tiingo_ws.quote_queue', quote_queue)):
                await crypto_feed(tickers)
            trade_feeds = []
            quote_feeds = []
            while not trade_queue.empty():
                trade_feeds.append(await trade_queue.get())
            while not quote_queue.empty():
                quote_feeds.append(await quote_queue.get())
            return trade_feeds, quote_feeds
        trade_feeds,quote_feeds  = asyncio.run(run_test_crypto_feed())
        trade_feed = trade_feeds[0]['market_feed']
        quote_feed = quote_feeds[0]['market_feed']

        exp_quote_event_time = parser.isoparse('1990-02-22T16:35:45.725000+00:00')
        exp_quote_event_time = exp_quote_event_time.astimezone(constants.NY_TZ)
        exp_quote_data = {
            'asset_type': constants.ASSET_TYPE_CRYPTO,
            'event_type': constants.EVENT_TYPE_QUOTE,
            'symbol': 'btceth',
            'bid_size': 100000.0,
            'ask_size': 100000.0,
            'bid': 11.58626,
            'ask': 11.59447,
            'mid': 11.590365,
            'event_time': exp_quote_event_time,
            'vendor': 'tiingo',
            'source': 'tiingo_crypto',
            'created_at': dtt.now(constants.NY_TZ)
        }
        exp_trade_event_time = parser.isoparse('1990-01-22T17:37:32.177890+00:00')
        exp_trade_event_time = exp_trade_event_time.astimezone(constants.NY_TZ)
        exp_trade_data = {
            'asset_type': constants.ASSET_TYPE_CRYPTO,
            'event_type': constants.EVENT_TYPE_TRADE,
            'symbol': 'eurusd',
            'last_size': 49.1372016,
            'last_price': 1.174045259378473,
            'event_time': exp_trade_event_time,
            'vendor': 'tiingo',
            'source': 'tiingo_crypto',
            'created_at': dtt.now(constants.NY_TZ)
            }
        self.assertEqual(quote_feed['asset_type'], exp_quote_data['asset_type'])
        self.assertEqual(quote_feed['event_type'], exp_quote_data['event_type'])
        self.assertEqual(quote_feed['symbol'], exp_quote_data['symbol'])
        self.assertEqual(quote_feed['bid_size'], exp_quote_data['bid_size'])
        self.assertEqual(quote_feed['ask_size'], exp_quote_data['ask_size'])
        self.assertEqual(quote_feed['bid'], exp_quote_data['bid'])
        self.assertEqual(quote_feed['ask'], exp_quote_data['ask'])
        self.assertEqual(quote_feed['mid'], exp_quote_data['mid'])
        self.assertEqual(quote_feed['event_time'], exp_quote_data['event_time'])
        self.assertEqual(quote_feed['vendor'], exp_quote_data['vendor'])
        self.assertEqual(quote_feed['source'], exp_quote_data['source'])
        self.assertIsNotNone(quote_feed['created_at'])
        
        self.assertEqual(trade_feed['asset_type'], exp_trade_data['asset_type'])
        self.assertEqual(trade_feed['event_type'], exp_trade_data['event_type'])
        self.assertEqual(trade_feed['symbol'], exp_trade_data['symbol'])
        self.assertEqual(trade_feed['last_size'], exp_trade_data['last_size'])
        self.assertEqual(trade_feed['last_price'], exp_trade_data['last_price'])
        self.assertEqual(trade_feed['event_time'], exp_trade_data['event_time'])
        self.assertEqual(trade_feed['vendor'], exp_trade_data['vendor'])
        self.assertEqual(trade_feed['source'], exp_trade_data['source'])
        self.assertIsNotNone(trade_feed['created_at'])


if __name__ == '__main__':
    unittest.main()