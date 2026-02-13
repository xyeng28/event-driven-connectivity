import unittest
from unittest.mock import patch, AsyncMock, MagicMock
import asyncio
from src.core.raw_feed_consolidator import consolidate_queue, run_consolidator

class TestRawFeedConsolidator(unittest.IsolatedAsyncioTestCase):

    async def test_consolidate_queue(self):
        event_type = "trade"
        data = {"price": 100, "volume": 10, "created_at": "ts"}

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(side_effect=[
            {"market_feed": data},
            {"market_feed": data},
            {"market_feed": data},
            asyncio.CancelledError()  # Stop infinite loop
        ])
        mock_queue.task_done = MagicMock()

        with patch("src.core.raw_feed_consolidator.pd.DataFrame") as mock_df_class, \
             patch("src.core.raw_feed_consolidator.dtt") as mock_datetime:

            # Mock datetime
            mock_now = MagicMock()
            mock_now.strftime.return_value = "2026_01_01"
            mock_datetime.now.return_value = mock_now

            # Mock DataFrame instance
            mock_df = MagicMock()
            mock_df.columns.difference.return_value = []
            mock_df.duplicated.return_value = []
            mock_df.__getitem__.return_value = mock_df
            mock_df.drop_duplicates.return_value = None
            mock_df.to_hdf.return_value = None

            mock_df_class.return_value = mock_df

            with self.assertRaises(asyncio.CancelledError):
                await consolidate_queue(mock_queue, event_type, buffer_size=3)

            mock_df_class.assert_called_once()
            mock_df.to_hdf.assert_called_once()
            self.assertTrue(mock_queue.task_done.called)

    async def test_run_consolidator(self):
        with patch("src.core.raw_feed_consolidator.asyncio.gather", new_callable=AsyncMock) as mock_gather:
            await run_consolidator()
            mock_gather.assert_called_once()
