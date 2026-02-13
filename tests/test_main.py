import unittest
from unittest.mock import patch, AsyncMock
from src.main import startup

class TestMain(unittest.IsolatedAsyncioTestCase):
    async def test_startup(self):
        tickers = ["AAPL", "BTCUSD"]

        with patch("src.main.load_tickers", return_value=tickers) as mock_load, \
             patch("src.main.run_consolidator", new_callable=AsyncMock) as mock_consolidator, \
             patch("src.main.tiingo.iex_stocks_feed", new_callable=AsyncMock) as mock_iex, \
             patch("src.main.tiingo.crypto_feed", new_callable=AsyncMock) as mock_crypto, \
             patch("src.main.tiingo.fx_feed", new_callable=AsyncMock) as mock_fx, \
             patch("src.main.asyncio.create_task") as mock_create_task:

            await startup()

            mock_load.assert_called_once()
            self.assertEqual(mock_create_task.call_count, 4)
            mock_iex.assert_called_once_with(tickers)
            mock_crypto.assert_called_once_with(tickers)
            mock_fx.assert_called_once_with(tickers)
