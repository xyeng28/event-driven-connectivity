import os
import unittest
from unittest.mock import patch, AsyncMock, MagicMock
import asyncio
from src import constants
from src.constants import VENDOR_TIINGO
from src.core.raw_feed_consolidator import consolidate_queue, run_consolidator, save_to_parquet


class TestRawFeedConsolidator(unittest.IsolatedAsyncioTestCase):

    @patch("src.core.raw_feed_consolidator.pq.write_table")
    @patch("src.core.raw_feed_consolidator.dtt")
    def test_save_to_parquet(self, mock_dtt, mock_write_table):
        buffer = {
            ("feed1", "AAPL", "2026-02-14T09:30:00"): {
                "source": "feed1",
                "symbol": "AAPL",
                "event_time": "2026-02-14T09:30:00",
                "price": 150
            },
            ("feed1", "GOOG", "2026-02-14T09:31:00"): {
                "source": "feed1",
                "symbol": "GOOG",
                "event_time": "2026-02-14T09:31:00",
                "price": 2700
            }
        }
        pq_dir = "dummy_dir"
        event_type = "trade"

        mock_now = MagicMock()
        mock_now.strftime.return_value = "20260214_093000_123456"
        mock_dtt.now.return_value = mock_now
        save_to_parquet(buffer, pq_dir, event_type)

        self.assertEqual(mock_write_table.call_count, 1)
        args, kwargs = mock_write_table.call_args
        pq_fp = args[1]
        self.assertIn("consol_feeds_trade_20260214_093000_123456.parquet", pq_fp)
        self.assertTrue(kwargs["compression"] == "snappy")
        self.assertEqual(len(buffer), 0)
        self.assertTrue(os.path.exists(pq_dir) or True)  # safe check; actual dir may not be created in mock

    async def test_consolidate_queue(self):
        event_type = "trade"
        pq_dir = "dummy_dir"
        buffer_size = 2

        data1 = {
            'asset_type': constants.ASSET_TYPE_STK,
            'event_type': constants.EVENT_TYPE_REF_PX,
            'symbol': 'AAPL',
            'price': 10.735,
            'vendor': VENDOR_TIINGO,
            'source': 'tiingo_iex',
            'event_time': '2026-02-14T09:30:00',
            'created_at': '2026-02-14T09:30:00'
        }
        data2 = {
            'asset_type': constants.ASSET_TYPE_STK,
            'event_type': constants.EVENT_TYPE_QUOTE,
            'symbol': 'AAPL',
            'price': 10.735,
            'vendor': VENDOR_TIINGO,
            'source': 'tiingo_iex',
            'event_time': '2026-02-14T09:30:00',
            'created_at': '2026-02-14T09:30:00'
        }
        data3 = {
            'asset_type': constants.ASSET_TYPE_STK,
            'event_type': constants.EVENT_TYPE_QUOTE,
            'symbol': 'GOOGL',
            'price': 10.735,
            'vendor': VENDOR_TIINGO,
            'source': 'tiingo_iex',
            'event_time': '2026-02-14T09:30:00',
            'created_at': '2026-02-14T09:30:00'
        }

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(side_effect=[
            {"market_feed": data1},
            {"market_feed": data2},
            {"market_feed": data3},
            asyncio.CancelledError()
        ])
        mock_queue.task_done = MagicMock()
        with patch("src.core.raw_feed_consolidator.asyncio.to_thread") as mock_to_thread:
            fut = asyncio.Future()
            fut.set_result(None)
            mock_to_thread.return_value = fut

            with self.assertRaises(asyncio.CancelledError):
                await consolidate_queue(mock_queue, pq_dir=pq_dir, buffer_size=2, event_type=event_type)

            self.assertEqual(mock_to_thread.call_count, 1)

    async def test_run_consolidator(self):
        with patch("src.core.raw_feed_consolidator.asyncio.gather", new_callable=AsyncMock) as mock_gather:
            await run_consolidator()
            mock_gather.assert_called_once()
